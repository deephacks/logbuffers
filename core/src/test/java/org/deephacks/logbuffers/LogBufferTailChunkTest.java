package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.TailSchedule.TailScheduleChunk;
import org.deephacks.logbuffers.json.JacksonSerializer;
import org.deephacks.logbuffers.json.JacksonSerializer.A;
import org.deephacks.logbuffers.json.JacksonSerializer.PageViews;
import org.deephacks.logbuffers.json.JacksonSerializer.PageViews.Count;
import org.deephacks.logbuffers.json.JacksonSerializer.TailA;
import org.deephacks.logbuffers.protobuf.ProtoLog.PageView;
import org.deephacks.logbuffers.protobuf.ProtobufSerializer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
   * The general case with high throughput pageViewWrites and slower batch reader, where the tail process
   * logs over longer time periods in order to aggregate data into more manageable chunks.
   *
   * In the event where the backlog grows too fast, the tail will notice and process log chunks
   * faster in order to catch up as quickly as possible.
   */
  @Test
  public void slow_batch_reader() throws Exception {
    final int roundMs = 300;
    final long writeTimeMs = TimeUnit.SECONDS.toMillis(3);
    // test json and protobuf logs in same buffer
    final LogBuffer buffer = new Builder().basePath(basePath)
            .addSerializer(new JacksonSerializer())
            .addSerializer(new ProtobufSerializer()).build();
    final LogBuffer aggregate = new Builder().basePath(aggregatePath).addSerializer(new JacksonSerializer()).build();
    for (int i = 1; i < 3; i++) {
      long first = System.currentTimeMillis();
      final CountDownLatch latch = new CountDownLatch(1);
      Writers writers = new Writers(writeTimeMs, buffer, latch);
      writers.start();

      // start pageViewTail after writers to make sure "early" logs are captured
      PageViewTail pageViewTail = new PageViewTail(aggregate);
      // start up a second type tail to make sure different type interfere with each other
      TailA tailA = new TailA();
      if (i % 2 == 0) {
        System.out.println("forwardWithFixedDelay");
        TailSchedule pageViewSchedule = TailSchedule.builder(pageViewTail)
                .delay(roundMs, MILLISECONDS)
                .build();
        buffer.forwardWithFixedDelay(pageViewSchedule);
        TailSchedule tailASchedule = TailSchedule.builder(tailA)
                .delay(roundMs, MILLISECONDS)
                .build();
        buffer.forwardWithFixedDelay(tailASchedule);
      } else {
        final long chunk = 200;
        System.out.println("forwardTimeChunksWithFixedDelay");
        TailScheduleChunk pageViewSchedule = TailScheduleChunk.builder(pageViewTail)
                .delay(roundMs, MILLISECONDS)
                .chunkLength(chunk, MILLISECONDS)
                .build();
        buffer.forwardWithFixedDelay(pageViewSchedule);
        TailScheduleChunk tailASchedule = TailScheduleChunk.builder(tailA)
                .delay(roundMs, MILLISECONDS)
                .chunkLength(chunk, MILLISECONDS)
                .build();
        buffer.forwardWithFixedDelay(tailASchedule);
      }

      // wait for writers to finish
      latch.await();
      System.out.println("Writer done. size " + writers.pageViewWrites.size() + " idx " + writers.pageViewWrites.lastKey());
      final long now = System.currentTimeMillis();
      // wait for pageViewTail to finish or fail after timeout
      while (pageViewTail.reads.size() != writers.pageViewWrites.size()) {
        Thread.sleep(500);
        if ((System.currentTimeMillis() - now) >  TimeUnit.SECONDS.toMillis(10)) {
          throw new RuntimeException("Readers took too long. Maybe this machine is slow, but probably a bug.");
        }
      }

      // wait a bit to be certain that pageViewTail doesn't move past the write index
      Thread.sleep(roundMs);
      buffer.cancel(PageViewTail.class);
      buffer.cancel(TailA.class);
      // assert that pageViewTail and writer have same number of logs
      assertThat(pageViewTail.reads.size(), is(writers.pageViewWrites.size()));
      assertThat(tailA.logs.size(), is(writers.aWrites.size()));
      System.out.println("Readers... size " + pageViewTail.reads.size() + " idx " + pageViewTail.reads.lastKey());
      System.out.println("Round " + i + " success " + pageViewTail.reads.size());

      // check that page view aggregation by the pageViewTail logger is same as writer
      Logs<PageViews> select = aggregate.selectBackward(PageViews.class, first, System.currentTimeMillis());
      PageViews sum = new PageViews(select.getFirst().getFrom(), select.getLastLog().getTimestamp());
      for (PageViews pageViews : select.get()) {
        for (Count count : pageViews.getCounts().values()) {
          sum.increment(count.getUrl(), count.getIncrement());
        }
      }
      assertThat((int) sum.total(), is(writers.pageViewWrites.size()));
    }
  }

  public static class PageViewTail implements Tail<PageView> {
    private int failures = 3;
    private ConcurrentSkipListMap<Long, PageView> reads = new ConcurrentSkipListMap<>();
    private LogBuffer buffer;
    public PageViewTail(LogBuffer buffer) {
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
      for (Log<PageView> pageView : logs.getLogs()) {
        pageViews.increment(pageView.get().getUrl(), 1);
        LogRaw log = pageView.getLog();
        if (reads.putIfAbsent(log.getIndex(), pageView.get()) != null) {
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
    private ConcurrentSkipListMap<Long, PageView> pageViewWrites = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Long, A> aWrites = new ConcurrentSkipListMap<>();
    private CountDownLatch latch;
    public Writers(long writeTimeMs, LogBuffer buffer, CountDownLatch latch) {
      this.stopTime = System.currentTimeMillis() + writeTimeMs;
      this.buffer = buffer;
      this.latch = latch;
    }

    @Override
    public void run() {
      // enough to produce at least 100.000 pageViewWrites per seconds
      int numThreads = Runtime.getRuntime().availableProcessors();
      final CountDownLatch threadWait = new CountDownLatch(numThreads);
      for (int i = 0; i < numThreads; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            while (System.currentTimeMillis() < stopTime) {
              try {
                LogUtil.sleep(1);
                // protobuf object
                PageView pageView = PageView.newBuilder().setUrl(LogUtil.randomUrl()).setValue(1).build();
                LogRaw log = buffer.write(pageView);
                if (pageViewWrites.putIfAbsent(log.getIndex(), pageView) != null) {
                  throw new RuntimeException("Duplicate write index!");
                }
                if (log.getIndex() % 10000 == 0) {
                  System.out.println("Write index: " + log.getIndex());
                }
                // json object
                A a = new A(new Random().nextLong());
                log = buffer.write(a);
                if (aWrites.putIfAbsent(log.getIndex(), a) != null) {
                  throw new RuntimeException("Duplicate write index!");
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
