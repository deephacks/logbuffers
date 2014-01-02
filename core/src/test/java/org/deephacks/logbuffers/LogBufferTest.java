package org.deephacks.logbuffers;


import org.deephacks.logbuffers.LogBuffer.Builder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

  @Before
  public void before() throws IOException {
    logBuffer = new Builder().basePath(LogUtil.tmpDir()).build();
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
    Log log1 = logBuffer.write(c1);
    System.out.println(log1.getIndex());
    List<Log> select = logBuffer.select(0);
    assertArrayEquals(select.get(0).getContent(), log1.getContent());

    // write another
    Log log2 = logBuffer.write(c2);
    System.out.println(log2.getIndex());
    select = logBuffer.select(0);
    assertThat(select.size(), is(2));
    assertArrayEquals(select.get(0).getContent(), log1.getContent());
    assertArrayEquals(select.get(1).getContent(), log2.getContent());

    assertArrayEquals(logBuffer.get(log1.getIndex()).get().getContent(), log1.getContent());
    assertArrayEquals(logBuffer.get(log2.getIndex()).get().getContent(), log2.getContent());

    // forward index past first log
    select = logBuffer.select(1);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log2.getContent());
  }

  @Test
  public void test_write_read_period() throws Exception {
    long t1 = timestamp();
    Log log1 = logBuffer.write(c1);

    long t2 = timestamp();
    Log log2 = logBuffer.write(c2);

    long t3 = timestamp();
    Log log3 = logBuffer.write(c3);

    long t4 = timestamp();
    Log log4 = logBuffer.write(c4);

    long t5 = timestamp();

    List<Log> select = logBuffer.selectPeriod(0, t1);
    assertThat(select.size(), is(0));

    select = logBuffer.selectPeriod(t1, t2);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log1.getContent());

    select = logBuffer.selectPeriod(t2, t3);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log2.getContent());

    select = logBuffer.selectPeriod(t3, t4);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log3.getContent());

    select = logBuffer.selectPeriod(t4, t5);
    assertThat(select.size(), is(1));
    assertArrayEquals(select.get(0).getContent(), log4.getContent());

    select = logBuffer.selectPeriod(t5, System.currentTimeMillis());
    assertThat(select.size(), is(0));

    select = logBuffer.selectPeriod(t2, t4);
    assertThat(select.size(), is(2));
    assertArrayEquals(select.get(0).getContent(), log2.getContent());
    assertArrayEquals(select.get(1).getContent(), log3.getContent());

  }


  @Test
  public void test_manual_forward() throws IOException {

    // one log
    Log log1 = logBuffer.write(c1);
    logBuffer.forward(tail);
    assertThat(tail.logs.size(), is(1));
    assertArrayEquals(tail.logs.get(0).getContent(), log1.getContent());

    // write another
    Log log2 = logBuffer.write(c2);
    logBuffer.forward(tail);
    assertThat(tail.logs.size(), is(2));
    assertArrayEquals(tail.logs.get(1).getContent(), log2.getContent());

    // write multiple
    log1 = logBuffer.write(c1);
    log2 = logBuffer.write(c2);
    logBuffer.forward(tail);
    assertThat(tail.logs.size(), is(4));
    assertArrayEquals(tail.logs.get(2).getContent(), log1.getContent());
    assertArrayEquals(tail.logs.get(3).getContent(), log2.getContent());
  }


  @Test
  public void test_scheduled_forward() throws Exception {

    logBuffer.forwardWithFixedDelay(tail, 500, TimeUnit.MILLISECONDS);
    // one log
    Log log1 = logBuffer.write(c1);
    Thread.sleep(600);
    assertThat(tail.logs.size(), is(1));
    assertArrayEquals(tail.logs.get(0).getContent(), log1.getContent());

    // write another
    Log log2 = logBuffer.write(c2);
    Thread.sleep(600);

    assertThat(tail.logs.size(), is(2));
    assertArrayEquals(tail.logs.get(1).getContent(), log2.getContent());

    // write multiple
    log1 = logBuffer.write(c1);
    log2 = logBuffer.write(c2);
    Thread.sleep(600);

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

  public static class TailLog implements Tail<Log> {

    public List<Log> logs = new ArrayList<>();

    @Override
    public void process(Logs<Log> logs) {
      this.logs.addAll(logs.getObjects());
    }
  }

}
