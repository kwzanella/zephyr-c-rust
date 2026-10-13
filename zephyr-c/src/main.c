#include <stddef.h>
#include <stdint.h>
#include <sys/errno.h>
#include <zephyr/kernel.h>
#include <zephyr/sys/printk.h>
#include <zephyr/sys/crc.h>
#include <zephyr/random/random.h>
#include <zephyr/sys/clock.h>

#define BUF_SIZE 10
#define PRIORITY 5
#define STACKSIZE 1024

// Message delay to simulate computation
#define DELAY_MS 100

#define MS_TO_US 1000
#define PRODUCER_US ((DELAY_MS) * MS_TO_US)
#define CONSUMER_US ((DELAY_MS) * MS_TO_US)

/*
 * Prints once per X number of messages.
 * Used to avoid deadline timing interference.
 * The higher the value, the lower DEADLINE_MS can be.
 */
#define PRINT_RATE (200)

/*
 * Deadline must take into account several delays.
 * All delays have been combined to 50 ms. This value has been defined empirically.
 * The delays are:
 *     - Queueing (from producer and consumer);
 *     - Zephyr schedulling; and
 *     - logging delay (printk).
 */
#define DEADLINE_MS (DELAY_MS + 50)

// Timeout to wait emitting or accepting from channel
#define CHANNEL_TIMEOUT_MS (DEADLINE_MS)

// Message to be passed in the channel
typedef struct {
    uint32_t payload;
    uint32_t seqno;
    uint64_t deadline_ms;
    uint32_t checksum;  // Must remain last
} __attribute__((packed)) Message;

// Channel to be used by producer and consumer
K_MSGQ_DEFINE(g_channel, sizeof(Message), BUF_SIZE, __alignof__(Message));

// Producer thread
K_THREAD_STACK_DEFINE(g_producer_stack, STACKSIZE);
static struct k_thread g_producer_data;

// Consumer thread
K_THREAD_STACK_DEFINE(g_consumer_stack, STACKSIZE);
static struct k_thread g_consumer_data;

// Semaphore to sync starting threads
K_SEM_DEFINE(g_start_sem, 0, 2);

// Emits data into channel
int emit(Message *data)
{
    return k_msgq_put(&g_channel, data, K_NO_WAIT);
}

// Accepts message from channel
int accept(Message *msg)
{
    k_timeout_t timeout = K_MSEC(CHANNEL_TIMEOUT_MS);
    return k_msgq_get(&g_channel, msg, timeout);
}

// Simulates producer's computation
static void producer_compute(void)
{
    k_busy_wait(PRODUCER_US);
}

// Simulates consumer's computation
static void consumer_compute(void)
{
    k_busy_wait(CONSUMER_US);
}

// Builds message for producer
static void build_msg(Message *msg)
{
    static uint32_t seqno = 0;

    msg->seqno = seqno;
    msg->payload = sys_rand32_get();
    msg->deadline_ms = k_uptime_get() + DEADLINE_MS;

    msg->checksum = crc32_ieee(
        (uint8_t *)msg,
        offsetof(Message, checksum)
    );

    seqno++;  // Overflow wraps to zero
}

// Verifies message received in consumer
void verify_msg(Message *msg, uint64_t *checksum_errors, uint64_t *sequence_errors) {
    static uint32_t expected_seqno = 0;

    uint32_t calculated_checksum = crc32_ieee(
        (uint8_t *)msg,
        offsetof(Message, checksum)
    );

    if (calculated_checksum != msg->checksum) {
        (*checksum_errors)++;
    }
    if (msg->seqno != expected_seqno) {
        (*sequence_errors)++;
    }

    expected_seqno = msg->seqno + 1;
}

// Checks if deadline has been met
void check_deadline(Message *msg, uint64_t *deadline_misses, uint64_t *on_time) {
    uint64_t finish_ms = k_uptime_get();

    if (finish_ms > msg->deadline_ms) {
        (*deadline_misses)++;
    }
    else {
        (*on_time)++;
    }
}

// Print producer state
void print_producer(uint64_t *emitted, uint64_t *queue_full, uint64_t *channel_err) {
    printk("emitted: %llu, queue-full drops: %llu, channel err: %llu\n",
        (unsigned long long)*emitted,
        (unsigned long long)*queue_full,
        (unsigned long long)*channel_err);
}

// Print consumer state
void print_consumer(uint64_t *attempts, uint64_t *accepted,
    uint64_t *checksum_errors, uint64_t *sequence_errors,
    uint64_t *deadline_misses, uint64_t *on_time,
    uint64_t *receive_errors)
{
    printk("attempts: %llu, accepted: %llu, receive err: %llu, checksum err: %llu, "
            "sequence err: %llu, deadline misses: %llu, "
            "on time: %llu\n",
            (unsigned long long)*attempts,
            (unsigned long long)*accepted,
            (unsigned long long)*receive_errors,
            (unsigned long long)*checksum_errors,
            (unsigned long long)*sequence_errors,
            (unsigned long long)*deadline_misses,
            (unsigned long long)*on_time);
}

// Producer thread function
static void producer_function(void *p1, void *p2, void *p3)
{
    k_sem_take(&g_start_sem, K_FOREVER);

    printk("Producer compute: %u ms\n", (PRODUCER_US / 1000));

    uint64_t emitted = 0;
    uint64_t queue_full = 0;
    uint64_t channel_err = 0;
    while (1) {
        producer_compute();

        Message msg = {0};
        build_msg(&msg);

        int result;
        if (msg.seqno == 100U) {
            Message *heap_msg = k_malloc(sizeof(*heap_msg));
            if (heap_msg == NULL) {
                printk("FAULT_SETUP_FAILED use-after-free seqno=%u\n", msg.seqno);
                result = emit(&msg);
            } else {
                *heap_msg = msg;
                k_free(heap_msg);
                printk("FAULT use-after-free seqno=%u\n", msg.seqno);
                result = emit(heap_msg);  // Intentional read after free in k_msgq_put.
            }
        } else {
            result = emit(&msg);
        }
        if (result == 0) {
            emitted++;
        }
        else if (result == -EAGAIN || result == -ENOMSG) {
            queue_full++;
        }
        else { // Unexpected error
            channel_err++;
        }

        if ((emitted + queue_full) % PRINT_RATE == 0) {
            print_producer(&emitted, &queue_full, &channel_err);
        }
    }
}

// Consumer thread function
static void consumer_function(void *p1, void *p2, void *p3)
{
    k_sem_take(&g_start_sem, K_FOREVER);

    printk("Consumer compute: %u ms\n", (CONSUMER_US / 1000));

    uint64_t attempts = 0;
    uint64_t accepted = 0;
    uint64_t receive_errors = 0;
    uint64_t checksum_errors = 0;
    uint64_t sequence_errors = 0;
    uint64_t deadline_misses = 0;
    uint64_t on_time = 0;
    while (1) {
        Message msg = {0};
        attempts++;

        int result = accept(&msg);

        if (result == 0) {
            accepted++;

            verify_msg(&msg, &checksum_errors, &sequence_errors);
            consumer_compute();
            check_deadline(&msg, &deadline_misses, &on_time);
        }
        else {
            receive_errors++;
        }

        if (attempts % PRINT_RATE == 0) {
            print_consumer(&attempts, &accepted, &checksum_errors, &sequence_errors,
                           &deadline_misses, &on_time, &receive_errors);
        }
    }
}

int main(void)
{
    k_thread_create(&g_producer_data, g_producer_stack,
        K_THREAD_STACK_SIZEOF(g_producer_stack),
        producer_function, NULL, NULL, NULL,
        PRIORITY, 0, K_NO_WAIT);
    k_thread_name_set(&g_producer_data, "producer_task");

    k_thread_create(&g_consumer_data, g_consumer_stack,
        K_THREAD_STACK_SIZEOF(g_consumer_stack),
        consumer_function, NULL, NULL, NULL,
        PRIORITY, 0, K_NO_WAIT);
    k_thread_name_set(&g_consumer_data, "consumer_task");

    printk("Starting threads...\n");

    k_thread_start(&g_consumer_data);
    k_thread_start(&g_producer_data);
    k_sem_give(&g_start_sem);
    k_sem_give(&g_start_sem);

    return 0;
}
