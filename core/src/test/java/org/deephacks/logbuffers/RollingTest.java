package org.deephacks.logbuffers;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.deephacks.logbuffers.LogBufferTest.TailLog;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RollingTest {
  LogBuffer logBuffer;
  TailLog tail;
  String basePath;

  @Before
  public void before() throws Exception {
    if (logBuffer != null) {
      logBuffer.close();
    }
    this.basePath = LogUtil.cleanupTmpDir();
    logBuffer = LogBuffer.newBuilder()
      .secondly()
      .basePath(basePath)
      .build();

    tail = new TailLog();
  }

  @Test
  public void testSelectRolloverIndex() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    List<LogRaw> selected = logBuffer.select(written.getFirst().getIndex());
    for (int j = 0; j < written.size(); j++) {
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testSelectRolloverIndexWithBigMargins() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    List<LogRaw> selected = logBuffer.select(0, Long.MAX_VALUE);
    for (int j = 0; j < written.size(); j++) {
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testSelectRolloverIndexOutsideKnown() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    List<LogRaw> selected = logBuffer.select(0, 1);
    assertThat(selected.size(), is(0));
    long lastIndex = written.getLast().getIndex();
    selected = logBuffer.select(lastIndex + 1, lastIndex + 2);
    assertThat(selected.size(), is(0));
  }

  @Test
  public void testSelectRolloverForwardTime() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    long t1 = written.getFirst().getTimestamp();
    long t2 = written.getLast().getTimestamp();
    List<LogRaw> selected = logBuffer.selectForward(t1, t2);
    for (int j = 0; j < written.size(); j++) {
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testSelectForwardWithBigMargins() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    List<LogRaw> selected = logBuffer.selectForward(0, Long.MAX_VALUE);
    for (int j = 0; j < written.size(); j++) {
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testSelectForwardOutsideKnown() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    List<LogRaw> selected = logBuffer.selectForward(0, 1);
    assertThat(selected.size(), is(0));
    long lastTimestamp = written.getLast().getTimestamp();
    selected = logBuffer.selectForward(lastTimestamp + 1000, lastTimestamp + 2000);
    assertThat(selected.size(), is(0));
  }

  @Test
  public void testSelectRolloverBackwardTime() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    long t1 = written.getFirst().getTimestamp();
    long t2 = written.getLast().getTimestamp();
    Collections.reverse(written);
    List<LogRaw> selected = logBuffer.selectBackward(t2, t1);
    for (int j = 0; j < written.size(); j++) {
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testSelectBackwardWithBigMargins() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    Collections.reverse(written);
    List<LogRaw> selected = logBuffer.selectBackward(Long.MAX_VALUE, 0);
    for (int j = 0; j < written.size(); j++) {
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testSelectBackwardOutsideKnown() throws Exception {
    LinkedList<LogRaw> written = write(logBuffer);
    List<LogRaw> selected = logBuffer.selectBackward(1, 0);
    assertThat(selected.size(), is(0));
    long lastTimestamp = written.getLast().getTimestamp();
    selected = logBuffer.selectBackward(lastTimestamp + 2000, lastTimestamp + 1000);
    assertThat(selected.size(), is(0));
  }


  private LinkedList<LogRaw> write(LogBuffer logBuffer) throws Exception {
    LinkedList<LogRaw> written = new LinkedList<>();
    long first = System.currentTimeMillis();
    int i = 0;
    long stop = first + TimeUnit.SECONDS.toMillis(3);
    while (stop > System.currentTimeMillis()) {
      LogRaw log = logBuffer.write(toBytes(i++));
      written.add(log);
      // enough sleep to write thousands of logs
      Thread.sleep(1);
    }
    System.out.println("Wrote " + written.size());
    return written;
  }

  public static byte[] toBytes(final int n) {
    byte[] b = new byte[4];
    b[0] = (byte) (n >>> 24);
    b[1] = (byte) (n >>> 16);
    b[2] = (byte) (n >>> 8);
    b[3] = (byte) (n >>> 0);
    return b;
  }

  public static byte[] toBytes(final long n) {
    byte[] b = new byte[8];
    b[0] = (byte) (n >>> 56);
    b[1] = (byte) (n >>> 48);
    b[2] = (byte) (n >>> 40);
    b[3] = (byte) (n >>> 32);
    b[4] = (byte) (n >>> 24);
    b[5] = (byte) (n >>> 16);
    b[6] = (byte) (n >>> 8);
    b[7] = (byte) (n >>> 0);
    return b;
  }

}
