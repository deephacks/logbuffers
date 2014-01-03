package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.json.JacksonSerializer;
import org.deephacks.logbuffers.json.JacksonSerializer.PageViews;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.deephacks.logbuffers.json.JacksonSerializer.PageView;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class LogBufferTailChunkTest {
  static String basePath = "/tmp/LogBufferTailChunkTest";
  static String aggregatePath = "/tmp/LogBufferTailChunkTest/aggregate";

  static {
    new File(basePath).mkdirs();
    new File(aggregatePath).mkdirs();
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
    final int roundMs = 300;
    final long writeTimeMs = TimeUnit.SECONDS.toMillis(3);
    final LogBuffer buffer = new Builder().basePath(basePath).addSerializer(new JacksonSerializer()).build();
    final LogBuffer aggregate = new Builder().basePath(aggregatePath).addSerializer(new JacksonSerializer()).build();
    for (int i = 1; i < 3; i++) {
      long first = System.currentTimeMillis();
      final CountDownLatch latch = new CountDownLatch(1);
      Writers writers = new Writers(writeTimeMs, buffer, latch);
      writers.start();

      // start tail after writers to make sure "early" logs are captured
      TailPeriod tail = new TailPeriod(aggregate);
      if (i % 2 == 0) {
        System.out.println("forwardWithFixedDelay");
        buffer.forwardWithFixedDelay(tail, roundMs, TimeUnit.MILLISECONDS);
      } else {
        final long chunkMs = 200;
        System.out.println("forwardTimeChunksWithFixedDelay");
        buffer.forwardTimeChunksWithFixedDelay(tail, chunkMs, roundMs, TimeUnit.MILLISECONDS);
      }

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
      System.out.println("Round " + i + " success " + tail.reads.size());
      Logs<PageViews> select = aggregate.selectBackward(PageViews.class, first, System.currentTimeMillis());
      PageViews sum = new PageViews(select.getFirst().getFrom(), select.getLastLog().getTimestamp());
      for (PageViews pageViews : select.getObjects()) {
        for (PageView pageView : pageViews.getPageViews().values()) {
          sum.increment(pageView.getUrl(), pageView.getValue());
        }
      }
      assertThat((int) sum.total(), is(writers.writes.size()));
      System.out.println(sum);
    }
  }

  public static class TailPeriod implements Tail<PageView> {
    private int failures = 3;
    private ConcurrentSkipListMap<Long, PageView> reads = new ConcurrentSkipListMap<>();
    private LogBuffer buffer;
    public TailPeriod(LogBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void process(Logs<PageView> logs) {
      // simulate random errors in order to test fail-safety
      if (new Random().nextBoolean() && failures-- > 0) {
        throw new RuntimeException("random failure");
      }
      if (logs.isEmpty()) {
        return;
      }
      System.out.println("Read index: " + logs.getFirstLog().getIndex() + " ... " + logs.getLastLog().getIndex() + " size: " + logs.size());

      PageViews pageViews = new PageViews(logs.getFirstLog().getTimestamp(), logs.getLastLog().getTimestamp());
      for (PageView pageView : logs.getObjects()) {
        pageViews.increment(pageView.getUrl(), 1);
        Log log = logs.getLog(pageView);
        if (reads.putIfAbsent(log.getIndex(), pageView) != null) {
          throw new RuntimeException("Duplicate read index!");
        }
      }
      try {
        buffer.write(pageViews);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class Writers extends Thread {
    private long stopTime;
    private LogBuffer buffer;
    private ConcurrentSkipListMap<Long, PageView> writes = new ConcurrentSkipListMap<>();
    private CountDownLatch latch;
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
                PageView pageView = new PageView(LogUtil.randomUrl());
                Log log = buffer.write(pageView);
                if (writes.putIfAbsent(log.getIndex(), pageView) != null) {
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
