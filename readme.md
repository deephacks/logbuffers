loguffers - fast persistent buffers for streaming data. 


The purpose of logbuffers is to provide a simple, reliable, low latency disk storage with the intent of decoupling readers and writers, favoring batch processing in high throughput systems.

- Writing one million 36 character logs concurrently takes approximately 1.8 seconds on modern hardware.
- Reading the same one million logs takes approximately 200 milliseconds.
