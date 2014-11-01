package org.deephacks.logbuffers;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class LogBufferFindStartTimeTest {
  public static List<LogRaw> logs;
  private StartTimeTail startTimeTail;
  private LogBuffer buffer;
  String path;

  @Before
  public void setup() throws IOException {
    if (buffer != null) {
      buffer.close();
    }
    this.path = LogUtil.cleanupTmpDir();
    startTimeTail = new StartTimeTail();
    buffer = new LogBufferStub(LogBuffer.newBuilder()
      .hourly()
      .basePath(path));
    // logs must have unique index, but some may have same timestamp
    logs = new ArrayList<>();
    logs.add(new LogRaw(1L, new byte[0], 1, 0));
    logs.add(new LogRaw(1L, new byte[1], 1, 1));
    logs.add(new LogRaw(1L, new byte[2], 3, 2));
    logs.add(new LogRaw(1L, new byte[3], 3, 3));
    logs.add(new LogRaw(1L, new byte[4], 3, 4));
    logs.add(new LogRaw(1L, new byte[4], 3, 5));
    logs.add(new LogRaw(1L, new byte[4], 3, 6));
    logs.add(new LogRaw(1L, new byte[5], 5, 7));
    logs.add(new LogRaw(1L, new byte[6], 10, 8));
  }

  @Test
  public void test_start_time_before_first_log() throws Exception {
    // time 0 which is before any logs, should revert back to read index 0
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(0).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(9));
    assertEquals(startTimeTail.logs.get(0).getIndex(), logs.get(0).getIndex());
    assertEquals(startTimeTail.logs.get(8).getIndex(), logs.get(8).getIndex());
  }

  @Test
  public void test_start_time_after_last_log() throws Exception {
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(11).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(1));
    assertEquals(startTimeTail.logs.get(0).getIndex(), logs.get(8).getIndex());
  }

  @Test
  public void test_start_time_between_timestamp() throws Exception {
    // this matches 3 logs and the index should point to the lowest index
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(2).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(7));
    assertEquals(startTimeTail.logs.get(0).getIndex(), logs.get(2).getIndex());
    assertEquals(startTimeTail.logs.get(4).getIndex(), logs.get(6).getIndex());
    assertEquals(startTimeTail.logs.get(6).getIndex(), logs.get(8).getIndex());

    schedule = TailSchedule.builder(startTimeTail).startTime(7).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(1));
    assertEquals(startTimeTail.logs.get(0).getIndex(), logs.get(8).getIndex());
  }

  @Test
  public void test_start_time_same_timestamp() throws Exception {
    // this matches 3 logs and the index should point to the lowest index
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(3).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(7));
    assertEquals(startTimeTail.logs.get(0).getIndex(), logs.get(2).getIndex());
    assertEquals(startTimeTail.logs.get(4).getIndex(), logs.get(6).getIndex());

    schedule = TailSchedule.builder(startTimeTail).startTime(1).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(9));
    assertEquals(startTimeTail.logs.get(0).getIndex(), logs.get(0).getIndex());
    assertEquals(startTimeTail.logs.get(8).getIndex(), logs.get(8).getIndex());
  }



  @Test
  public void test_real_data() throws Exception {
    buffer = LogBuffer.newBuilder().basePath(path).build();
    byte[] c1 = LogUtil.randomLog();
    byte[] c2 = LogUtil.randomLog();
    byte[] c3 = LogUtil.randomLog();
    byte[] c4 = LogUtil.randomLog();

    long t1 = timestamp();
    LogRaw log1 = buffer.write(c1);

    long t2 = timestamp();
    LogRaw log2 = buffer.write(c2);

    long t3 = timestamp();
    LogRaw log3 = buffer.write(c3);

    long t4 = timestamp();
    LogRaw log4 = buffer.write(c4);

    long t5 = timestamp();

    StartTimeTail startTimeTail = new StartTimeTail();
    // time 0 which is before any logs, should revert back to read index 0
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(0).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(4));
    assertArrayEquals(startTimeTail.logs.get(0).getContent(), log1.getContent());
    assertArrayEquals(startTimeTail.logs.get(1).getContent(), log2.getContent());
    assertArrayEquals(startTimeTail.logs.get(2).getContent(), log3.getContent());
    assertArrayEquals(startTimeTail.logs.get(3).getContent(), log4.getContent());

    // cancel tail, then skip exactly to log3
    startTimeTail = new StartTimeTail();
    buffer.cancel(StartTimeTail.class);
    schedule = TailSchedule.builder(startTimeTail).startTime(log3.getTimestamp()).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(2));
    assertArrayEquals(startTimeTail.logs.get(0).getContent(), log3.getContent());
    assertArrayEquals(startTimeTail.logs.get(1).getContent(), log4.getContent());

    // cancel tail, set start time just before log3
    startTimeTail = new StartTimeTail();
    buffer.cancel(StartTimeTail.class);
    schedule = TailSchedule.builder(startTimeTail).startTime(t3).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(2));
    assertArrayEquals(startTimeTail.logs.get(0).getContent(), log3.getContent());
    assertArrayEquals(startTimeTail.logs.get(1).getContent(), log4.getContent());

    // cancel tail, set start time just after log3
    startTimeTail = new StartTimeTail();
    buffer.cancel(StartTimeTail.class);
    schedule = TailSchedule.builder(startTimeTail).startTime(log3.getTimestamp() + 1).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(1));
    assertArrayEquals(startTimeTail.logs.get(0).getContent(), log4.getContent());


    // cancel tail, then set to end of time which should revert back to last read index
    // which should be at the end from previous forward operation
    startTimeTail = new StartTimeTail();
    schedule = TailSchedule.builder(startTimeTail).startTime(Long.MAX_VALUE).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(0));

    // cancel tail, then skip to timestamp of log4
    startTimeTail = new StartTimeTail();
    buffer.cancel(StartTimeTail.class);
    schedule = TailSchedule.builder(startTimeTail).startTime(log4.getTimestamp()).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(1));
    assertArrayEquals(startTimeTail.logs.get(0).getContent(), log4.getContent());
  }

  @Ignore
  public void perf_test() throws Exception {
    LogBuffer buffer = LogBuffer.newBuilder().basePath(path).build();
    Stopwatch stopwatch = new Stopwatch().start();
    long oneMillionIndex = 1000000;
    long oneMillionTimestamp = 0;
    long prev = 0;
    for (int i = 0; i < 10000000; i++) {
      if (i == oneMillionIndex) {
        oneMillionTimestamp = System.currentTimeMillis();
      }
      if (i % 1000000 == 0) {
        System.out.println(i);
      }
      // check that index is sequential
      long tmpIndex = buffer.write(new byte[]{1}).getIndex();
      if (tmpIndex != 0) {
        if (prev + 1 != tmpIndex) {
          throw new IllegalArgumentException(prev + " " + tmpIndex);
        }
        prev = tmpIndex;
      }
    }
    System.out.println(stopwatch.elapsed(TimeUnit.MICROSECONDS));
    stopwatch = new Stopwatch().start();
    Long index = buffer.findStartTimeIndex(0);
    System.out.println("index " + index + " found in " +  stopwatch.elapsed(TimeUnit.MICROSECONDS) + " micros");
    stopwatch = new Stopwatch().start();
    index = buffer.findStartTimeIndex(System.currentTimeMillis());
    System.out.println("index " + index + " found in " +  stopwatch.elapsed(TimeUnit.MICROSECONDS) + " micros");
    for (int j = 0; j < 10; j++) {
      stopwatch = new Stopwatch().start();
      index = buffer.findStartTimeIndex(oneMillionTimestamp);
      System.out.println("index " + index + " found in " +  stopwatch.elapsed(TimeUnit.MICROSECONDS) + " micros");
      LogRaw log = buffer.get(index).get();
      LogRaw oneMillionLog = buffer.get(oneMillionIndex).get();
      assertThat(log.getTimestamp(), is(oneMillionLog.getTimestamp()));
    }
  }

  long timestamp() throws InterruptedException {
    Thread.sleep(10);
    long time = System.currentTimeMillis();
    Thread.sleep(10);
    return time;
  }


  public static class LogBufferStub extends LogBuffer {

    private LogBufferStub(Builder builder) throws IOException {
      super(builder);
    }

    @Override
    public long getWriteIndex() throws IOException {
      // write index always point to next write index, not the current read index
      return logs.size();
    }

    @Override
    Optional<LogRaw> get(long index) throws IOException {
      return Optional.fromNullable(logs.get((int) index));
    }

    @Override
    public <T> Optional<LogRaw> getNext(Class<T> cls, long index) throws IOException {
      return Optional.fromNullable(logs.get((int) index));
    }

    @Override
    Optional<Long> peekTimestamp(long index) throws IOException {
      return Optional.fromNullable(get(index).get().getTimestamp());
    }
  }

  public static class StartTimeTail implements Tail<LogRaw> {

    public List<LogRaw> logs = new ArrayList<>();

    @Override
    public void process(Logs<LogRaw> logs) {
      this.logs = logs.get();
    }
  }
}
