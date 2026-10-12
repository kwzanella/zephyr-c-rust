#include <zephyr/random/random.h>
#include <zephyr/sys/crc.h>

#include <stdint.h>
#include <stdint.h>
#include <stddef.h>

uint32_t rust_random_u32(void)
{
    return sys_rand32_get();
}

uint32_t rust_crc32(const uint8_t *data, size_t len)
{
    return crc32_ieee(data, len);
}
