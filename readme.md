### logbuffers - fast persistent buffers for streaming data. 

[![Build Status](https://travis-ci.org/deephacks/logbuffers.svg?branch=master)](https://travis-ci.org/deephacks/logbuffers)

The purpose of logbuffers is to provide simple, reliable, low latency persistence buffers for high throughput data feeds. 

Every buffer consist of an ordered, immutable sequence of logs that is continually appended to — much like a commit log.
Every log is given an increasing (but not necessarily sequential) index number (position/offset) that uniquely identifies each log within the buffer.

Buffers are stored in continuously rolling files (daily, hourly or minutely) and every buffer retain every log written to it - whether or not it have been consumed - until the physical file is manually removed. 

Normally a consumer will advance its index linearly as it reads logs, but can in fact consume logs in any order it likes. A consumer may want reset the index to an arbitrary position in order to reprocess logs in failure scenarios for example.

### Performance

- Writing one million 36 character logs concurrently takes approximately 1.8 seconds on modern hardware.
- Reading the same one million logs takes approximately 200 milliseconds.
- 1 hour processing downtime in a system that produce 10000 logs/sec (36m backlog) will be catched up in less than a minute.


### Reliability

- Buffers do not loose data if the JVM crash and core dumps.
- Logs may be lost during power failures or a total OS crash.
- Buffers can be configured for synchronous writes and survive power failures at the cost of performance.


### Streaming logs

A consumer may use the index to stream logs. A buffer does not track consumed logs so the consumer itself may need to keep track of log indexes to avoid loosing or processing logs twice. Streams works just like a regular lazy [java.util.stream.Stream](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html) pipeline; computation on logs is only performed when the terminal operation is initiated, and logs are consumed only as needed. Memory will
not be exhausted unless the stream query itself cache too much data during execution.

```java
// create a buffer
LogBuffer buffer = LogBuffer.newBuilder()
  .hourly()
  .basePath("/tmp/logbuffer")
  .build();

// write to buffer
buffer.write("log message".getBytes());

// stream logs from index 0 up until the most recent log written.
java.util.stream.Stream<Log> stream = buffer.find(Query.atLeastIndex(0)).stream();

// stream logs between index 10 - 20
java.util.stream.Stream<Log> stream = buffer.find(Query.closedIndex(10, 20)).stream();

// stream logs between time t1 and time t2
java.util.stream.Stream<Log> stream = buffer.find(Query.closedTime(t1, t2)).stream();

```
### Parallel processing of logs

Since logs are stored in a directory structure according to a rolling interval (as seen below) each time interval can be processed in parallel. Parallel scanning scales linearly with number of CPU cores and speeds up processing tremendously.

```sh
$ ls -al /tmp/logbuffer
drwxr-xr-x  2 java java  4096 Nov 22 20:00 2014-11-22-20
drwxr-xr-x  2 java java  4096 Nov 22 21:00 2014-11-22-21
drwxr-xr-x  2 java java  4096 Nov 22 22:00 2014-11-22-22
```

A buffer with a base path directed to this directory can then process all of them in parallel.

```java
LogBuffer buffer = LogBuffer.newBuilder()
  .hourly()
  .basePath("/tmp/logbuffer")
  .build();

// process each log directory in parallel
java.util.stream.Stream<Log> stream = buffer.parallel().stream();
```

### Tailing logs

A buffer can track log processing through tail instances. Each tail instance have a separate and persistent read index. The read index can be forwarded periodically and/or manually. Logs are considered successfully processed by the tail instance if no exception occured during the process() method. Any exception force the logs to be re-delivered next round, maybe along with additional unseen/new logs.


```java

LogTail tail = new LogTail();

// manual tail forwarding
buffer.forward(tail)

// schedule periodic tail forwarding
buffer.forwardWithFixedDelay(tail, 500, TimeUnit.MILLISECONDS);

// cancel tail schedule
buffer.cancel(tail);

class LogTail implements Tail {
  public void process(Logs logs) { 
    // group, aggregate, sum or whatever 
  }
}

```


### Object logbuffer

More complicated log structures can be written and read using [vals](https://github.com/deephacks/vals) encoding in the same way as a raw logs. Note that each buffer track its specific type ONLY. This is by design so that different log types can be processed and reported separately.

It is of course also possible to write logs encoded using Protobuf, Avro or JSON but this will be slower than using  [vals](https://github.com/deephacks/vals), which have been optimized for log buffers.

```java

@Val
public interface PageView extends Encodable {
  @Id(0) String getUrl();
  @Id(1) Long getUserId();
}

LogBuffer buffer = LogBuffer.newBuilder()
  .minutely()
  .build();
  
logBuffer.write(new PageViewBuilder().withUrl("www.google.com").withUserId(1L).build());
logBuffer.write(new PageViewBuilder().withUrl("www.facebook.com").withUserId(1L).build());
logBuffer.write(new PageViewBuilder().withUrl("www.yahoo.com").withUserId(2L).build());
logBuffer.write(new PageViewBuilder().withUrl("www.yahoo.com").withUserId(2L).build());
logBuffer.write(new PageViewBuilder().withUrl("www.google.com").withUserId(4L).build());
logBuffer.write(new PageViewBuilder().withUrl("www.google.com").withUserId(5L).build());
logBuffer.write(new PageViewBuilder().withUrl("www.google.com").withUserId(1L).build());

Map<String, Long> pageViewsPerUrl = logBuffer.find(Query.atLeastIndex(0))
    .stream(PageViewBuilder::parseFrom)
    .collect(groupingBy(PageView::getUrl, counting()));

assertThat(pageViewsPerUrl.get("www.google.com"), is(4L));
assertThat(pageViewsPerUrl.get("www.facebook.com"), is(1L));
assertThat(pageViewsPerUrl.get("www.yahoo.com"), is(2L));

Map<String, Set<Long>> uniqueVisitorsPerUrl = logBuffer.find(Query.atLeastIndex(0))
  .stream(PageViewBuilder::parseFrom)
  .collect(groupingBy(PageView::getUrl, mapping(PageView::getUserId, toSet())));

assertThat(uniqueVisitorsPerUrl.get("www.google.com").size(), is(3));
assertThat(uniqueVisitorsPerUrl.get("www.facebook.com").size(), is(1));
assertThat(uniqueVisitorsPerUrl.get("www.yahoo.com").size(), is(1));

```
