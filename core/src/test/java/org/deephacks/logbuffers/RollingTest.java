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

  private LinkedList<LogRaw> write(LogBuffer logBuffer) throws Exception {
    LinkedList<LogRaw> written = new LinkedList<>();
    long first = System.currentTimeMillis();
    int i = 0;
    long stop = first + TimeUnit.SECONDS.toMillis(3);
    while (stop > System.currentTimeMillis()) {
      LogRaw log = logBuffer.write(toBytes(i++));
      written.add(log);
      Thread.sleep(250);
    }
    return written;
  }

  private byte[] toBytes(final int n) {
    byte[] b = new byte[4];
    b[0] = (byte) (n >>> 24);
    b[1] = (byte) (n >>> 16);
    b[2] = (byte) (n >>> 8);
    b[3] = (byte) (n >>> 0);
    return b;
  }
}
