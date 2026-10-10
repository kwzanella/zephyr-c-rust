# zephyr-c-rust
C vs. Rust: Compile-time memory safety and reliability in Zephyr RTOS.

## Compilation
In the root of the repo, execute one of the commands described below.

To compile C application: `west build -p -b qemu_cortex_m3 zephyr-c`

To compile Rust application: `west build -p -b qemu_cortex_m3 zephyr-rust`

## Running on QEMU
Both applications can be executed with the same command after compilation. Just run `west build -t run`
