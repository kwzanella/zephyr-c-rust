#![no_std]

// Cargo tries to detect configs that have typos in them.  Unfortunately, the Zephyr Kconfig system
// uses a large number of Kconfigs and there is no easy way to know which ones might conceivably be
// valid.  This prevents a warning about each cfg that is used.
#![allow(unexpected_cfgs)]

extern crate alloc;

use zephyr_panic as _;
use alloc::boxed::Box;
use zephyr::printkln;
use zephyr::sync::channel::{bounded, Receiver, Sender};
use zephyr::time::{Duration, Forever};
use zephyr::raw::{k_busy_wait, k_uptime_get};
use zephyr::sys::sync::Semaphore;

const BUF_SIZE: usize = 10;
const PRIORITY: i32 = 5;
const STACKSIZE: usize = 1024;

// Message delay to simulate computation
const DELAY_MS: u32 = 100;

const PRODUCER_US: u32 = (DELAY_MS) * 1000;
const CONSUMER_US: u32 = (DELAY_MS) * 1000;

/*
 * Prints once per X number of messages.
 * Used to avoid deadline timing interference.
 * The higher the value, the lower DEADLINE_MS can be.
 */
const PRINT_RATE: u64 = 200;

/*
 * Deadline must take into account several delays.
 * All delays have been combined to 50 ms. This value has been defined empirically.
 * The delays are:
 *     - Queueing (from producer and consumer);
 *     - Zephyr schedulling; and
 *     - logging delay (printk).
 */
const DEADLINE_MS: u64 = DELAY_MS as u64 + 50;

// Timeout to wait emitting or accepting from channel
const CHANNEL_TIMEOUT_MS: u64 = DEADLINE_MS;

// Semaphore to sync starting threads
static START_SEM: Semaphore = Semaphore::new(0, 2);

// Message to be passed in the channel
#[repr(C, packed)]
struct Message {
    payload: u32,
    seqno: u32,
    deadline_ms: u64,
    checksum: u32,
}

// C wrapper to call crc32_ieee (from zephyr_c_wrapper.c)
unsafe extern "C" {
    fn rust_crc32(data: *const u8, len: usize) -> u32;
}

// C wrapper to call sys_rand32_get (from zephyr_c_wrapper.c)
unsafe extern "C" {
    fn rust_random_u32() -> u32;
}

// Calculates message checksum
fn message_checksum(msg: &Message) -> u32 {
    // SAFETY: Checksum starts after 16 bytes of message
    let bytes = unsafe {
        core::slice::from_raw_parts(msg as *const Message as *const u8, 16)
    };

    // SAFETY: bytes points to 16 readable bytes for the duration
    // of the call, and rust_crc32 only reads the supplied buffer.
    unsafe {
        rust_crc32(bytes.as_ptr(), bytes.len())
    }
}

// Builds message for producer
fn build_msg(seqno: u32) -> Message {
    // SAFETY: Use of C wrapper. No pointers or memory operations required
    let payload = unsafe { rust_random_u32() };

    // SAFETY: Get uptime from C raw (zephyr::raw)
    let now_ms = unsafe { k_uptime_get() as u64 };

    let mut msg = Message {
        payload,
        seqno,
        deadline_ms: now_ms + DEADLINE_MS,
        checksum: 0,
    };

    msg.checksum = message_checksum(&msg);
    msg
}

// Simulates producer's computation
fn producer_compute() {
    // SAFETY: The requested busy-wait duration is a valid u32.
    unsafe {
        k_busy_wait(PRODUCER_US);
    }
}

// Simulates consumer's computation
fn consumer_compute() {
    // SAFETY: The requested busy-wait duration is a valid u32.
    unsafe {
        k_busy_wait(CONSUMER_US);
    }
}

// Verifies message received in consumer
fn verify_msg(msg: &Message, expected_seqno: &mut u32, checksum_errors: &mut u64, sequence_errors: &mut u64) {
    if message_checksum(msg) != msg.checksum {
        *checksum_errors += 1;
    }

    if msg.seqno != *expected_seqno {
        *sequence_errors += 1;
    }

    *expected_seqno = msg.seqno.wrapping_add(1);
}

// Checks if deadline has been met
fn check_deadline(msg: &Message, deadline_misses: &mut u64, on_time: &mut u64) {
    // SAFETY: Get uptime from C raw (zephyr::raw)
    let finish_ms = unsafe { k_uptime_get() as u64 };

    if finish_ms > msg.deadline_ms {
        *deadline_misses += 1;
    }
    else {
        *on_time += 1;
    }
}

// Emits data into channel
fn emit(tx: &Sender<Message>, msg: Message) -> Result<(), impl core::fmt::Debug> {
    tx.try_send(msg)
}

// Print producer state
fn print_producer(emitted: u64, send_errors: u64) {
    printkln!(
        "emitted: {}, send errors: {}",
        emitted, send_errors
    );
}

// Producer thread function
#[zephyr::thread(stack_size = STACKSIZE)]
fn producer(tx: Sender<Message>) {
    let mut seqno = 0u32;
    let mut emitted = 0u64;
    let mut send_errors = 0u64;

    START_SEM.take(Forever).unwrap();

    printkln!("Producer compute: {} ms", PRODUCER_US / 1000);

    loop {
        producer_compute();

        let msg = if seqno == 100 {
            let heap_msg = Box::new(build_msg(seqno));
            drop(heap_msg);
            *heap_msg  // Intentional safe-Rust compile error: use after drop.
        } else {
            build_msg(seqno)
        };
        seqno = seqno.wrapping_add(1);

        match emit(&tx, msg) {
            Ok(()) => emitted += 1,
            Err(_) => send_errors += 1,
        }

        if (emitted + send_errors).is_multiple_of(PRINT_RATE) {
            print_producer(emitted, send_errors);
        }
    }
}

// Accepts message from channel
fn accept(rx: &Receiver<Message>, timeout: Duration) -> Result<Message, impl core::fmt::Debug> {
    rx.recv_timeout(timeout)
}

// Print consumer state
fn print_consumer(
    attempts: u64,
    accepted: u64,
    receive_errors: u64,
    checksum_errors: u64,
    sequence_errors: u64,
    deadline_misses: u64,
    on_time: u64,
) {
    printkln!(
        "attempts: {}, accepted: {}, receive err: {}, \
         checksum err: {}, sequence err: {}, \
         deadline misses: {}, on time: {}",
        attempts,
        accepted,
        receive_errors,
        checksum_errors,
        sequence_errors,
        deadline_misses,
        on_time,
    );
}

// Consumer thread function
#[zephyr::thread(stack_size = STACKSIZE)]
fn consumer(rx: Receiver<Message>) {
    let mut attempts = 0u64;
    let mut accepted = 0u64;
    let mut receive_errors = 0u64;
    let mut checksum_errors = 0u64;
    let mut sequence_errors = 0u64;
    let mut deadline_misses = 0u64;
    let mut on_time = 0u64;
    let mut expected_seqno = 0u32;

    START_SEM.take(Forever).unwrap();

    let timeout = Duration::from_ticks(CHANNEL_TIMEOUT_MS);

    printkln!("Consumer compute: {} ms", CONSUMER_US / 1000);

    loop {
        attempts += 1;

        match accept(&rx, timeout) {
            Ok(msg) => {
                accepted += 1;

                verify_msg(
                    &msg,
                    &mut expected_seqno,
                    &mut checksum_errors,
                    &mut sequence_errors,
                );

                consumer_compute();

                check_deadline(
                    &msg,
                    &mut deadline_misses,
                    &mut on_time,
                );
            }
            Err(_) => receive_errors += 1,
        }

        if attempts.is_multiple_of(PRINT_RATE) {
            print_consumer(
                attempts,
                accepted,
                receive_errors,
                checksum_errors,
                sequence_errors,
                deadline_misses,
                on_time,
            );
        }
    }
}

#[no_mangle]
extern "C" fn rust_main() {
    // SAFETY: `rust_main` runs once during application startup before any rust tasks
    // are spawned, so the global logger is initialized before concurrent use.
    unsafe {
        zephyr::set_logger().unwrap();
    }

    let (tx, rx) = bounded::<Message>(BUF_SIZE);

    printkln!("Starting threads...");

    let producer_thread = producer(tx);
    producer_thread.set_priority(PRIORITY);
    producer_thread.start();

    let consumer_thread = consumer(rx);
    consumer_thread.set_priority(PRIORITY);
    consumer_thread.start();

    START_SEM.give();
    START_SEM.give();
}
