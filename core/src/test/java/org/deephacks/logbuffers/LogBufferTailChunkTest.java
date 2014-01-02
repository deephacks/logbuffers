package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.json.JacksonSerializer;
import org.deephacks.logbuffers.json.JacksonSerializer.A;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LogBufferTailChunkTest {
  static String dir = "/tmp/LogBufferTailChunkTest";
  static AtomicLong counter = new AtomicLong();
  static {
    new File(dir).mkdirs();
  }
  /**
   * The general case with high throughput writes and slower batch reader, where the tail process
   * logs over longer time periods in order to aggregate data into more manageable chunks.
   *
   * In the event where the backlog grows too fast, the tail will notice and process log chunks
   * faster in order to catch up as quickly as possible.
   */
  @Test
  public void slow_batch_reader() throws Exception {
    final long chunkMs = 300;
    final int roundMs = 300;
    final long writeTimeMs = TimeUnit.SECONDS.toMillis(3);
    final LogBuffer buffer = new Builder().basePath(dir).addSerializer(new JacksonSerializer()).build();
    final CountDownLatch latch = new CountDownLatch(1);
    Writers writers = new Writers(writeTimeMs, buffer, latch);
    writers.start();

    // start tail after writers to make sure "early" logs are captured
    TailPeriod tail = new TailPeriod();
    buffer.forwardTimeChunksWithFixedDelay(tail, chunkMs, roundMs, TimeUnit.MILLISECONDS);
    latch.await();
    System.out.println("Writer done. size " + writers.writes.size() + " idx " + writers.writes.lastKey());
    final long now = System.currentTimeMillis();
    while (tail.reads.size() != writers.writes.size()) {
      Thread.sleep(500);
      if ((System.currentTimeMillis() - now) >  TimeUnit.SECONDS.toMillis(10)) {
        throw new RuntimeException("Readers took too long. Maybe this machine is slow, but probably a bug.");
      }
    }
    // wait a bit to be certain that tail doesn't move past the write index
    Thread.sleep(roundMs);
    assertThat(tail.reads.size(), is(writers.writes.size()));
    buffer.cancel(TailPeriod.class);
    System.out.println("Readers... size "+ tail.reads.size() + " idx " + tail.reads.lastKey());
    System.out.println("SUCCESS: "+ tail.reads.size());
  }

  public static class TailPeriod implements Tail<A> {
    int failures = 3;
    ConcurrentSkipListMap<Long, A> reads = new ConcurrentSkipListMap<>();
    @Override
    public void process(Logs<A> logs) {
      // simulate random errors in order to test fail-safety
      if (new Random().nextBoolean() && failures-- > 0) {
        throw new RuntimeException("random failure");
      }
      if (!logs.isEmpty()) {
        System.out.println("Read index: " + logs.getFirstLog().getIndex() + " ... " + logs.getLastLog().getIndex() + " size: " + reads.size());
      }
      for (A a : logs.getObjects()) {
        if (reads.putIfAbsent(logs.getLog(a).getIndex(), a) != null) {
          throw new RuntimeException("Duplicate read index!");
        }
      }
    }
  }

  public static class Writers extends Thread {
    long stopTime;
    LogBuffer buffer;
    ConcurrentSkipListMap<Long, A> writes = new ConcurrentSkipListMap<>();
    CountDownLatch latch;
    public Writers(long writeTimeMs, LogBuffer buffer, CountDownLatch latch) {
      this.stopTime = System.currentTimeMillis() + writeTimeMs;
      this.buffer = buffer;
      this.latch = latch;
    }

    @Override
    public void run() {
      // enough to produce at least 100.000 writes per seconds
      int numThreads = Runtime.getRuntime().availableProcessors() * 2;
      final CountDownLatch threadWait = new CountDownLatch(numThreads);
      for (int i = 0; i < numThreads; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            while (System.currentTimeMillis() < stopTime) {
              try {
                LogUtil.sleep(1);
                A a = new A(counter.getAndIncrement());
                Log log = buffer.write(a);
                if (writes.putIfAbsent(log.getIndex(), a) != null) {
                  throw new RuntimeException("Duplicate write index!");
                }
                if (log.getIndex() % 10000 == 0) {
                  System.out.println("Write index: " + log.getIndex());
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            threadWait.countDown();
          }
        }).start();
      }
      try {
        threadWait.await();
      } catch (InterruptedException e) {
      }
      latch.countDown();
    }
  }
}
