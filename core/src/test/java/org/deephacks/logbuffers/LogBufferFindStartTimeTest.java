package org.deephacks.logbuffers;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class LogBufferFindStartTimeTest {
  private LogBufferTest.StartTimeTail startTimeTail;
  private LogBuffer buffer;
  String path;
  @Before
  public void setup() throws IOException {
    if (buffer != null) {
      buffer.close();
    }
    this.path = LogUtil.cleanupTmpDir();
    startTimeTail = new LogBufferTest.StartTimeTail();
    startTimeTail.logs.clear();
    buffer = LogBuffer.newBuilder()
      .hourly()
      .basePath(path)
      .build();

  }

  @Test
  public void testStartTimeBeforeFirstLog() throws Exception {
    LinkedList<Log> written = LogUtil.write(buffer, 0, 200);
    // fromTime 0 which is before any logs, should revert back to read index 0
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(0).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(written.size()));
    for (int j = 0; j < written.size(); j++) {
      assertThat(startTimeTail.logs.get(j), is(written.pollFirst()));
    }
  }

  @Test
  public void testStartTimeAfterLastLog() throws Exception {
    LinkedList<Log> written = LogUtil.write(buffer, 0, 200);
    TailSchedule schedule = TailSchedule.builder(startTimeTail).startTime(written.getLast().getTimestamp() + 1).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(0));
  }

  @Test
  public void testStartTimeBetweenTimestamp() throws Exception {
    Log first = buffer.write(new byte[]{1});
    TailSchedule schedule = TailSchedule.builder(startTimeTail).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(1));
    assertThat(startTimeTail.logs.get(0), is(first));
    Thread.sleep(1);

    Log second = buffer.write(new byte[]{2});
    startTimeTail.logs.clear();
    schedule = TailSchedule.builder(startTimeTail).startTime(first.getTimestamp() + 1).build();
    buffer.forward(schedule);
    assertThat(startTimeTail.logs.size(), is(1));
    assertEquals(startTimeTail.logs.get(0), second);
  }
}
