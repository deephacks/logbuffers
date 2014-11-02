package org.deephacks.logbuffers;


import org.deephacks.logbuffers.LogBuffer.Builder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class LogBufferTest {
  LogBuffer logBuffer;
  TailLog tail;
  byte[] c1 = LogUtil.randomLog();
  byte[] c2 = LogUtil.randomLog();
  byte[] c3 = LogUtil.randomLog();
  byte[] c4 = LogUtil.randomLog();
  String basePath;

  @Before
  public void before() throws Exception {
    if (logBuffer != null) {
      logBuffer.close();
    }
    this.basePath = LogUtil.cleanupTmpDir();
    logBuffer = LogBuffer.newBuilder()
      .hourly()
      .basePath(basePath).build();

    tail = new TailLog();
  }

  @After
  public void after() throws IOException {
    logBuffer.cancel(TailLog.class);
    logBuffer.close();
  }

  @Test
  public void test_write_read() throws IOException {
    // one log
    LogRaw log1 = logBuffer.write(c1);
    long startIndex = log1.getIndex();
    List<LogRaw> select = logBuffer.select(startIndex);
    assertArrayEquals(select.get(0).getContent(), log1.getContent());

    // write another
    tail.logs.clear();
    LogRaw log2 = logBuffer.write(c2);
    select = logBuffer.select(startIndex);
    assertThat(select.size(), is(2));
    assertArrayEquals(select.get(0).getContent(), log1.getContent());
    assertArrayEquals(select.get(1).getContent(), log2.getContent());

    assertArrayEquals(logBuffer.get(log1.getIndex()).get().getContent(), log1.getContent());
    assertArrayEquals(logBuffer.get(log2.getIndex()).get().getContent(), log2.getContent());

    // forward index past first log
    select = logBuffer.select(startIndex + 1);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log2.getContent());
  }

  @Test
  public void test_write_read_period() throws Exception {
    long t1 = timestamp();
    LogRaw log1 = logBuffer.write(c1);

    long t2 = timestamp();
    LogRaw log2 = logBuffer.write(c2);

    long t3 = timestamp();
    LogRaw log3 = logBuffer.write(c3);

    long t4 = timestamp();
    LogRaw log4 = logBuffer.write(c4);

    long t5 = timestamp();

    List<LogRaw> select = logBuffer.selectBackward(t1, 0);
    assertThat(select.size(), is(0));

    select = logBuffer.selectBackward(t2, t1);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log1.getContent());

    select = logBuffer.selectBackward(t3, t2);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log2.getContent());

    select = logBuffer.selectBackward(t4, t3);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log3.getContent());

    select = logBuffer.selectBackward(t5, t4);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log4.getContent());

    select = logBuffer.selectBackward(System.currentTimeMillis(), t5);
    assertThat(select.size(), is(0));

    select = logBuffer.selectBackward(t4, t2);
    assertThat(select.size(), is(2));
    assertArrayEquals(select.get(1).getContent(), log2.getContent());
    assertArrayEquals(select.get(0).getContent(), log3.getContent());
  }

  @Test
  public void test_manual_forward() throws IOException {
    TailSchedule schedule = TailSchedule.builder(tail).build();
    // one log
    LogRaw log1 = logBuffer.write(c1);
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(1));
    assertArrayEquals(tail.logs.get(0).getContent(), log1.getContent());

    // write another
    LogRaw log2 = logBuffer.write(c2);
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(2));
    assertArrayEquals(tail.logs.get(1).getContent(), log2.getContent());

    // write multiple
    log1 = logBuffer.write(c1);
    log2 = logBuffer.write(c2);
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(4));
    assertArrayEquals(tail.logs.get(2).getContent(), log1.getContent());
    assertArrayEquals(tail.logs.get(3).getContent(), log2.getContent());
  }

  @Test
  public void test_scheduled_forward() throws Exception {
    // one log
    LogRaw log1 = logBuffer.write(c1);

    int delay = 250;
    long sleep = 500;
    TailSchedule tailSchedule = TailSchedule.builder(tail).delay(delay, TimeUnit.MILLISECONDS).build();
    logBuffer.forwardWithFixedDelay(tailSchedule);

    Thread.sleep(sleep);
    assertThat(tail.logs.size(), is(1));
    assertArrayEquals(tail.logs.get(0).getContent(), log1.getContent());

    // write another
    LogRaw log2 = logBuffer.write(c2);
    Thread.sleep(sleep);

    assertThat(tail.logs.size(), is(2));
    assertArrayEquals(tail.logs.get(1).getContent(), log2.getContent());

    // write multiple
    log1 = logBuffer.write(c1);
    log2 = logBuffer.write(c2);
    Thread.sleep(sleep);

    assertThat(tail.logs.size(), is(4));
    assertArrayEquals(tail.logs.get(2).getContent(), log1.getContent());
    assertArrayEquals(tail.logs.get(3).getContent(), log2.getContent());

  }

  long timestamp() throws InterruptedException {
    Thread.sleep(10);
    long time = System.currentTimeMillis();
    Thread.sleep(10);
    return time;
  }

  public static class TailLog implements Tail<LogRaw> {

    public List<LogRaw> logs = new ArrayList<>();

    @Override
    public void process(Logs<LogRaw> logs) {
      this.logs.addAll(logs.get());
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
