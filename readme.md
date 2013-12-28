### logbuffers - fast persistent buffers for streaming data. 


The purpose of logbuffers is to provide simple, reliable, low latency persistence for high throughput data feeds. 

Every buffer consist of an ordered, immutable sequence of logs that is continually appended to — much like a commit log.
Every log is given a sequential index number (position/offset) that uniquely identifies each log within the buffer.

Buffers are stored in continuously rolling files and every buffer retain every log written to it - whether or not it have been consumed - until the physical file is manually removed. 

Normally a consumer will advance its index linearly as it reads logs, but can in fact consume logs in any order it likes. A consumer may want reset the index to an arbitrary position in order to reprocess logs in failure scenarios for example.

- Writing one million 36 character logs concurrently takes approximately 1.8 seconds on modern hardware.
- Reading the same one million logs takes approximately 200 milliseconds.
- 1 hour processing downtime in a system that produce 10000 logs/sec (36m backlog) will be catched up in less than a minute.

### selecting logs

The manual way for a consumer to process logs, where the consumer iself must keep track of which logs have been processed.

```java
// create a buffer
LogBuffer buffer = new Builder().basePath("/tmp/logbuffer").build();

// write to buffer
buffer.write("log message".getBytes());

// select all logs from index 0 up until the most recent log written.
List<Log> logs = logBuffer.select(0);

// select all logs between index 10 - 20
List<Log> logs = logBuffer.select(10, 20);

```


### tailing logs

Tail utility that keep track of what logs have been processed. Forwards log processing periodically, notifying the tail each round. Successfully processed log indexes are persisted to local disk. Logs are considered successfully processed if no exception occur during processing. If an exception occur, the logs will be delivered next round, maybe along with additional unseen/new logs.


```java
new LogBufferTail(buffer, new LogTail()).forwardWithFixedDelay(500, TimeUnit.MILLISECONDS);

class LogTail implements Tail<Log> {
  public void process(List<Log> logs) { 
    // group, aggregate, sum or whatever 
  }
}

```
