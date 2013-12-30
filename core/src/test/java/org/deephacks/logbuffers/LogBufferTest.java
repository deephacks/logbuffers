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
  Log log1 = LogUtil.randomLog(1);
  Log log2 = LogUtil.randomLog(2);
  Log log3 = LogUtil.randomLog(3);
  Log log4 = LogUtil.randomLog(4);
  Log log5 = LogUtil.randomLog(5);

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
    log1 = logBuffer.write(log1);
    List<Log> select = logBuffer.select(0);
    assertThat(select.get(0), is(log1));

    // write another
    log2 = logBuffer.write(log2);
    select = logBuffer.select(0);
    assertThat(select.size(), is(2));
    assertThat(select.get(0), is(log1));
    assertThat(select.get(1), is(log2));

    // forward index past first log
    select = logBuffer.select(1);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(log2));
  }

  @Test
  public void test_write_read_period() throws IOException {
    // one log
    log1 = logBuffer.write(log1);
    log2 = logBuffer.write(log2);
    log3 = logBuffer.write(log3);
    log4 = logBuffer.write(log4);
    log5 = logBuffer.write(log5);

    List<Log> select = logBuffer.selectPeriod(0, 0);
    assertThat(select.size(), is(0));

    select = logBuffer.selectPeriod(1, 1);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(log1));

    select = logBuffer.selectPeriod(2, 2);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(log2));

    select = logBuffer.selectPeriod(3, 3);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(log3));

    select = logBuffer.selectPeriod(4, 4);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(log4));

    select = logBuffer.selectPeriod(5, 5);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(log5));

    select = logBuffer.selectPeriod(6, System.nanoTime());
    assertThat(select.size(), is(0));

    select = logBuffer.selectPeriod(3, 5);
    assertThat(select.size(), is(3));
    assertThat(select.get(0), is(log3));
    assertThat(select.get(1), is(log4));
    assertThat(select.get(2), is(log5));
  }


  @Test
  public void test_manual_forward() throws IOException {
    // one log
    log1 = logBuffer.write(log1);
    logBuffer.forward(tail);
    assertThat(tail.logs.size(), is(1));
    assertThat(tail.logs.get(0), is(log1));

    // write another
    log2 = logBuffer.write(log2);
    logBuffer.forward(tail);
    assertThat(tail.logs.size(), is(2));
    assertThat(tail.logs.get(1), is(log2));

    // write multiple
    log1 = logBuffer.write(log1);
    log2 = logBuffer.write(log2);
    logBuffer.forward(tail);
    assertThat(tail.logs.size(), is(4));
    assertThat(tail.logs.get(2), is(log1));
    assertThat(tail.logs.get(3), is(log2));
  }


  @Test
  public void test_scheduled_forward() throws Exception {
    logBuffer.forwardWithFixedDelay(tail, 500, TimeUnit.MILLISECONDS);
    // one log
    log1 = logBuffer.write(log1);
    Thread.sleep(600);
    assertThat(tail.logs.size(), is(1));
    assertThat(tail.logs.get(0), is(log1));

    // write another
    log2 = logBuffer.write(log2);
    Thread.sleep(600);

    assertThat(tail.logs.size(), is(2));
    assertThat(tail.logs.get(1), is(log2));

    // write multiple
    log1 = logBuffer.write(log1);
    log2 = logBuffer.write(log2);
    Thread.sleep(600);

    assertThat(tail.logs.size(), is(4));
    assertThat(tail.logs.get(2), is(log1));
    assertThat(tail.logs.get(3), is(log2));
  }

  public static class TailLog implements Tail<Log> {

    public List<Log> logs = new ArrayList<>();

    @Override
    public void process(List<Log> logs) {
      this.logs.addAll(logs);
    }
  }

}
