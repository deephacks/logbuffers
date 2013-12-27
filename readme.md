### loguffers - fast persistent buffers for streaming data. 


The purpose of logbuffers is to provide a simple, reliable, low latency disk storage with the intent of decoupling readers and writers, favoring batch processing in high throughput systems.

- Writing one million 36 character logs concurrently takes approximately 1.8 seconds on modern hardware.
- Reading the same one million logs takes approximately 200 milliseconds.



### selecting logs

```java
// create a buffer
LogBuffer buffer = new Builder().build();

// write to buffer
buffer.write("log message".getBytes());

// select all logs from index 0 up until the most recent log written.
List<Log> logs = logBuffer.select(0);

// select all logs between index 10 - 20
List<Log> logs = logBuffer.select(10, 20);

```


### tailing logs

```java
// forwards the log processing periodically by notifying the tail each round.
new LogBufferTail(logBuffer, new LogTail()).forwardWithFixedDelay(500, TimeUnit.MILLISECONDS);


public static class LogTail implements Tail<Log> {
  // called once every 0.5 seconds with unseen/new logs
  public void process(List<Log> logs) { 
    // logs are not lost if exception occur, will be delivered next round
    // and maybe along with additional unseen/new logs.
  }

  public String getName() { return "unique name"; }
}

```
