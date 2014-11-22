package org.deephacks.logbuffers;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.fail;
import static org.deephacks.logbuffers.LogBufferTest.TailLog;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
  public void testFindIndexRollover() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer);
    Thread.sleep(1000);
    LinkedList<Log> selected = logBuffer.find(Query.atLeastIndex(written.getFirst().getIndex())).toLinkedList();
    for (int j = 0; j < written.size(); j++) {
      assertThat(selected.get(j), is(written.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testFindIndexRolloverWithBigMargins() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer);
    LinkedList<Log> selected = logBuffer.find(Query.atLeastIndex(0)).toLinkedList();
    for (int j = 0; j < written.size(); j++) {
      selected.get(j).getTimestamp();
      assertThat(written.get(j), is(selected.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testFindIndexRolloverOutsideKnown() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer);
    LinkedList<Log> selected = logBuffer.find(Query.closedIndex(0, 1)).toLinkedList();
    assertThat(selected.size(), is(0));
    long lastIndex = written.getLast().getIndex();
    selected = logBuffer.find(Query.closedIndex(lastIndex + 1, lastIndex + 2)).toLinkedList();
    assertThat(selected.size(), is(0));
  }

  @Test
  public void testFindIndexRolloverForcingSameMilli() throws Exception {
    // write at full machine speed
    LinkedList<Log> written = LogUtil.write(logBuffer, 0, 1500);
    long startIndex = written.getFirst().getIndex();
    long stopIndex = written.getLast().getIndex();
    LinkedList<Log> selected = logBuffer.find(Query.closedIndex(startIndex, stopIndex)).toLinkedList();
    // millions of logs are written so assert "leniently" to avoid waiting irritation
    for (int j = 0; j < written.size(); j = j + 100000) {
      assertThat(written.get(j).getIndex(), is(selected.get(j).getIndex()));
    }
    // consistency assert
    assertThat(selected.size(), is(written.size()));
    assertThat(selected.getFirst().getIndex(), is(written.getFirst().getIndex()));
    assertThat(selected.getLast().getIndex(), is(written.getLast().getIndex()));
  }

  @Test
  public void testFindTimeRollover() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer);
    long t1 = written.getFirst().getTimestamp();
    long t2 = written.getLast().getTimestamp();
    LinkedList<Log> selected = logBuffer.find(Query.closedTime(t1, t2)).toLinkedList();
    for (int j = 0; j < written.size(); j++) {
      assertThat(selected.get(j), is(written.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testFindTimeWithBigMargins() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer);
    LinkedList<Log> selected = logBuffer.find(Query.closedTime(0, System.currentTimeMillis())).toLinkedList();
    for (int j = 0; j < written.size(); j++) {
      assertThat(selected.get(j), is(written.get(j)));
    }
    assertThat(selected.size(), is(written.size()));
  }

  @Test
  public void testFindTimeOutsideKnown() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer);
    LinkedList<Log> selected = logBuffer.find(Query.closedTime(0, 1)).toLinkedList();
    assertThat(selected.size(), is(0));
    long lastTimestamp = written.getLast().getTimestamp();
    long count = logBuffer.find(Query.closedTime(lastTimestamp + 1000, lastTimestamp + 2000)).stream().count();
    assertThat(count, is(0L));
  }

  @Test
  public void testFindTimeForcingSameMilli() throws Exception {
    LinkedList<Log> written = LogUtil.write(logBuffer, 0, 1500);
    long now = System.currentTimeMillis();
    long from = now - (now % 1000);
    long to = from + 100;
    LinkedList<Log> selected = logBuffer.find(Query.closedTime(from, to)).toLinkedList();
    assertTrue(!selected.isEmpty());
    for (Log log : written) {
      if (log.getTimestamp() >= from && log.getTimestamp() <= to) {
        assertThat(selected.pollFirst(), is(log));
      }
    }
    assertTrue(selected.isEmpty());
  }
}
