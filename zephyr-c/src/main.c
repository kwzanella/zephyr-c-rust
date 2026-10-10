#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <zephyr/kernel.h>
#include <zephyr/sys/printk.h>
#include <zephyr/sys/crc.h>

#define BUF_SIZE 10
#define INACTIVE -1
#define PRIORITY 5
#define STACKSIZE 1024

#define MS_TO_US 1000

#define PRODUCER_US (150 * MS_TO_US)
#define CONSUMER_US (200 * MS_TO_US)

#define CHANNEL_PERIOD_MS 10

#define DEADLINE_MS 5

// TODO Align struct (probably)
typedef struct {
    char payload[32];;    // Payload to check for corruption
    uint32_t seqno;       // Sequence number
    uint32_t checksum;    // Checksum to check for payload corruption
    uint64_t deadline_ms;  // Deadline for processing
} Message;

K_MSGQ_DEFINE(channel, sizeof(Message), BUF_SIZE, 1);

K_THREAD_STACK_DEFINE(producer_stack, STACKSIZE);
static struct k_thread producer_data;

K_THREAD_STACK_DEFINE(consumer_stack, STACKSIZE);
static struct k_thread consumer_data;

// Semaphore to sync thread start
K_SEM_DEFINE(start_sem, 0, 2);

uint8_t emit(Message *data)
{
    return k_msgq_put(&channel, (void*)data, K_NO_WAIT);
}

uint8_t accept(Message *msg)
{
    return k_msgq_get(&channel, (void*)msg, K_FOREVER);
}

void producer_compute()
{
    k_busy_wait(PRODUCER_US);
}

void consumer_compute()
{
    k_busy_wait(CONSUMER_US);
}

void print_message(const Message *msg)
{
    printk("Payload: %s\nSeqNo: %u\nDeadline: %llu\nChecksum: %u\n\n",
        msg->payload,
        msg->seqno,
        msg->deadline_ms,
        msg->checksum
    );
}

void producer_function(void *p1, void *p2, void *p3)
{
    k_sem_take(&start_sem, K_FOREVER);

    uint32_t seqno = 0;
    uint8_t payload = 0;
    uint64_t emitted = 0;
    uint64_t failed = 0;
    while (1) {
        producer_compute();

        Message msg = {
            .seqno = seqno,
            .deadline_ms = k_uptime_get() + DEADLINE_MS,
            .checksum = crc32_ieee(&payload, sizeof(payload))
        };
        snprintf(msg.payload, sizeof(msg.payload), "%c", (char)payload);

        if (emit(&msg) == 0)
        {
            emitted++;
        }
        else {
            failed++;
        }

        if (payload == 0xFF)
        {
            payload = 0;
        }
        if (seqno == 0xFFFFFFFF)
        {
            seqno = 0;
        }
        seqno++;
        payload++;
        printk("Emitted: %llu\n", emitted);
        printk("Failed: %llu\n", failed);
    }
}

void consumer_function(void *p1, void *p2, void *p3)
{
    k_sem_take(&start_sem, K_FOREVER);

    Message msg;
    uint64_t accepted = 0;
    uint64_t failed = 0;
    while (1) {
        if (accept(&msg) == 0)
        {
            accepted++;
        }
        else {
            failed++;
        }
        consumer_compute();
        // print_message(&msg);
        // printk("Accepted: %llu\n", accepted);
        // printk("Failed: %llu\n", failed);
    }
}

int main(void)
{
    // Producer task creation
    k_thread_create(&producer_data, producer_stack,
            K_THREAD_STACK_SIZEOF(producer_stack),
            producer_function, NULL, NULL, NULL,
            PRIORITY, 0, K_NO_WAIT);

    k_thread_name_set(&producer_data, "producer_task");

    // Consumer task creation
    k_thread_create(&consumer_data, consumer_stack,
        K_THREAD_STACK_SIZEOF(consumer_stack),
        consumer_function, NULL, NULL, NULL,
        PRIORITY, 0, K_NO_WAIT);

    k_thread_name_set(&consumer_data, "consumer_task");

    printk("Starting threads...\n");

    // Start tasks
    k_thread_start(&consumer_data);
    k_thread_start(&producer_data);

    // Give semaphore so threads start in sync
    k_sem_give(&start_sem);
    k_sem_give(&start_sem);
    return 0;
}
