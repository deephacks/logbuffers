package org.deephacks.logbuffers;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
      .secondly()
      .basePath(basePath).build();

    tail = new TailLog();
  }

  @After
  public void after() throws IOException {
    logBuffer.close();
  }

  @Test
  public void testWriteRead() throws Exception {
    // one log
    Log log1 = logBuffer.write(c1);
    long startIndex = log1.getIndex();
    Query query = Query.atLeastIndex(startIndex);
    LinkedList<Log> select = logBuffer.find(query).toLinkedList();
    assertEquals(select.getFirst().getIndex(), log1.getIndex());
    assertArrayEquals(select.getFirst().getContent(), log1.getContent());

    // write another
    Log log2 = logBuffer.write(c2);
    select = logBuffer.find(query).toLinkedList();
    assertThat(select.size(), is(2));
    assertEquals(select.get(0).getIndex(), log1.getIndex());
    assertEquals(select.get(1).getIndex(), log2.getIndex());

    assertThat(logBuffer.getIndex(log1.getIndex()).get().getIndex(), is(log1.getIndex()));
    assertThat(logBuffer.getIndex(log2.getIndex()).get().getIndex(), is(log2.getIndex()));

    // forward index past first log
    query = Query.atLeastIndex(log2.getIndex());
    select = logBuffer.find(query).toLinkedList();
    assertThat(select.size(), is(1));
    assertEquals(select.get(0).getIndex(), log2.getIndex());
  }

  @Test
  public void testWriteReadPeriod() throws Exception {
    long t1 = timestamp();
    Log log1 = logBuffer.write(c1);

    long t2 = timestamp();
    Log log2 = logBuffer.write(c2);

    long t3 = timestamp();
    Log log3 = logBuffer.write(c3);

    long t4 = timestamp();
    Log log4 = logBuffer.write(c4);

    long t5 = timestamp();

    long count = logBuffer.find(Query.closedTime(0, t1)).stream().count();
    assertThat(count, is(0L));

    LinkedList<Log> select = logBuffer.find(Query.closedTime(t1, t2)).toLinkedList();
    assertThat(select.size(), is(1));
    assertThat(select.getFirst().getIndex(), is(log1.getIndex()));

    select = logBuffer.find(Query.closedTime(t2, t3)).toLinkedList();
    assertThat(select.size(), is(1));
    assertThat(select.getFirst().getIndex(), is(log2.getIndex()));

    select = logBuffer.find(Query.closedTime(t3, t4)).toLinkedList();
    assertThat(select.size(), is(1));
    assertThat(select.getFirst().getIndex(), is(log3.getIndex()));

    select = logBuffer.find(Query.closedTime(t4, t5)).toLinkedList();
    assertThat(select.size(), is(1));
    assertThat(select.getFirst().getIndex(), is(log4.getIndex()));

    select = logBuffer.find(Query.closedTime(t5, System.currentTimeMillis())).toLinkedList();
    assertThat(select.size(), is(0));

    select = logBuffer.find(Query.closedTime(t2, t4)).toLinkedList();
    assertThat(select.size(), is(2));
    assertThat(select.getFirst().getIndex(), is(log2.getIndex()));
    assertThat(select.get(1).getIndex(), is(log3.getIndex()));
  }

  @Test
  public void testManualForward() throws Exception {
    TailSchedule schedule = TailSchedule.builder(tail).build();
    // one log
    Log log1 = logBuffer.write(c1);
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(1));
    assertThat(tail.logs.get(0).getIndex(), is(log1.getIndex()));

    Thread.sleep(1000);

    // write another
    Log log2 = logBuffer.write(c2);
    logBuffer.forward(schedule);

    assertThat(tail.logs.size(), is(2));
    assertThat(tail.logs.get(1).getIndex(), is(log2.getIndex()));
    assertArrayEquals(log2.getContent(), tail.logs.get(1).getContent());

    // write multiple
    log1 = logBuffer.write(c1);
    log2 = logBuffer.write(c2);
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(4));
    assertArrayEquals(tail.logs.get(2).getContent(), log1.getContent());
    assertArrayEquals(tail.logs.get(3).getContent(), log2.getContent());
  }


  @Test
  public void testManualForwardUtf8() throws Exception {
    TailSchedule schedule = TailSchedule.builder(tail).build();
    // one log
    Log log1 = logBuffer.write("1");
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(1));
    assertThat(tail.logs.get(0).getUtf8(), is("1"));

    // write another
    Log log2 = logBuffer.write("2");
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(2));
    assertThat(tail.logs.get(1).getUtf8(), is("2"));
    assertArrayEquals(log2.getContent(), tail.logs.get(1).getContent());

    // write multiple
    log1 = logBuffer.write("3");
    log2 = logBuffer.write("4");
    logBuffer.forward(schedule);
    assertThat(tail.logs.size(), is(4));
    assertThat(tail.logs.get(2).getUtf8(), is("3"));
    assertThat(tail.logs.get(3).getUtf8(), is("4"));
  }


  @Test
  public void testScheduledForward() throws Exception {
    // one log
    Log log1 = logBuffer.write(c1);

    int delay = 250;
    long sleep = 800;
    TailSchedule tailSchedule = TailSchedule.builder(tail)
      .delay(delay, TimeUnit.MILLISECONDS).build();
    logBuffer.forwardWithFixedDelay(tailSchedule);

    Thread.sleep(sleep);
    assertThat(tail.logs.size(), is(1));
    assertArrayEquals(tail.logs.get(0).getContent(), log1.getContent());

    // write another
    Log log2 = logBuffer.write(c2);
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


  @Test
  public void testFindTimeBeforeData() throws Exception {
    logBuffer.find(Query.atLeastTime(0)).stream().count();
    Log log1 = logBuffer.write(c1);
    LinkedList<Log> logs = logBuffer.find(Query.atLeastTime(0)).toLinkedList();
    assertThat(logs.size(), is(1));
    logs.getFirst().getTimestamp();
    assertThat(log1, is(logs.getFirst()));
  }

  @Test
  public void testFindIndexBeforeData() throws Exception {
    logBuffer.find(Query.atLeastIndex(0)).stream().count();
    Log log1 = logBuffer.write(c1);
    LinkedList<Log> logs = logBuffer.find(Query.atLeastIndex(0)).toLinkedList();
    assertThat(logs.size(), is(1));
    logs.getFirst().getTimestamp();
    assertThat(log1, is(logs.getFirst()));
  }

  @Test
  public void testParallel() throws Exception {
    Log[] written = LogUtil.writeList(logBuffer, 1, 5000).toArray(new Log[0]);
    Log[] logs = logBuffer.parallel().stream()
      .sorted().collect(Collectors.toList()).toArray(new Log[0]);
    assertArrayEquals(logs, written);
  }

  @Test
  public void testOneWriteAndOneTailBuffer() throws Exception {
    LogBuffer readBuffer = LogBuffer.newBuilder()
      .secondly()
      .basePath(basePath)
      .build();
    Log log = logBuffer.write(new byte[]{1});
    Logs logs = readBuffer.find(Query.atLeastIndex(log.getIndex()));
    assertThat(logs.stream().findFirst().get(), is(log));
  }


  @Test
  public void testSepecificRangeDirectory() throws Exception {
    Log log1 = logBuffer.write(c1);
    String time = RollingRanges.SECOND_FORMAT.format(new Date(log1.getTimestamp()));
    LogBuffer buffer = LogBuffer.newBuilder().basePath("/tmp/logBufferTest/" + time).build();
    LinkedList<Log> logs = buffer.find(Query.atLeastIndex(0)).toLinkedList();
    assertThat(logs.size(), is(1));
    logs.getFirst().getTimestamp();
    assertThat(log1, is(logs.getFirst()));
  }


  @Test
  public void testTwoTails() throws Exception {
    ReadTail tail1 = new ReadTail();
    TailSchedule schedule1 = TailSchedule.builder(tail1)
      .delay(500, TimeUnit.MILLISECONDS)
      .build();
    logBuffer.forwardWithFixedDelay(schedule1);
    ReadTail tail2 = new ReadTail();
    TailSchedule schedule2 = TailSchedule.builder(tail2)
      .delay(500, TimeUnit.MILLISECONDS)
      .build();

    logBuffer.forwardWithFixedDelay(schedule2);

    List<Log> written = LogUtil.writeList(logBuffer, 10, 3000);
    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() < now + 5000) {
      if (tail1.reads.size() + tail2.reads.size() == written.size()) {
        return;
      }
      Thread.sleep(100);
    }
    fail(tail1.reads.size() + tail2.reads.size() + " != " + written.size());
  }


  @Test
  public void testTailResume() throws Exception {
    Long lastRead = null;
    for (int i = 1; i <= 3; i++) {
      logBuffer = LogBuffer.newBuilder()
        .secondly()
        .basePath(basePath)
        .build();
      ReadTail tail1 = new ReadTail();
      TailSchedule schedule1 = TailSchedule.builder(tail1)
        .delay(500, TimeUnit.MILLISECONDS)
        .build();
      logBuffer.forwardWithFixedDelay(schedule1);
      Log[] written = LogUtil.writeList(logBuffer, 10, 3000).toArray(new Log[0]);
      long now = System.currentTimeMillis();
      while (System.currentTimeMillis() < now + 5000) {
        if (tail1.reads.size() == written.length) {
          break;
        }
        Thread.sleep(100);
      }
      Log[] logs = tail1.reads.values().toArray(new Log[0]);
      System.out.println("Round " + i);
      assertArrayEquals(written, logs);

      logBuffer.close();

    }
  }

  @Test
  public void testVals() throws Exception {
    LinkedList<Val1> vals = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      Map<TimeUnit, Integer> map = new HashMap<>();
      map.put(TimeUnit.DAYS, 1);
      map.put(TimeUnit.MINUTES, 2);
      Val1 val = new Val1Builder()
        .withStringList(Arrays.asList("1", "2", "3"))
        .withByteArray(new byte[]{1, 2, 3})
        .withString("string")
        .withPLong(1L)
        .withEnumIntegerMap(map)
        .build();
      vals.add(val);
      logBuffer.write(val);
    }

    logBuffer.find(Query.atLeastIndex(0))
      .stream(Val1Builder::parseFrom)
      .forEach(val -> assertThat(val, is(vals.pollFirst())));
  }


  long timestamp() throws InterruptedException {
    Thread.sleep(10);
    long time = System.currentTimeMillis();
    Thread.sleep(10);
    return time;
  }

  public static class TailLog implements Tail {
    public List<Log> logs = new ArrayList<>();

    @Override
    public void process(Logs logs) {
      LinkedList<Log> processed = logs.toLinkedList();
      this.logs.addAll(processed);
    }
  }

  public static class StartTimeTail implements Tail {

    public List<Log> logs = new ArrayList<>();

    @Override
    public void process(Logs logs) {
      this.logs.addAll(logs.toLinkedList());
    }
  }

  public static class ReadTail implements Tail {
    private ConcurrentSkipListMap<Long, Log> reads = new ConcurrentSkipListMap<>();

    @Override
    public void process(Logs logs) {
      LinkedList<Log> list = logs.toLinkedList();
      if (list.isEmpty()) {
        return;
      }
      System.out.println("index: " + list.getFirst().getIndex() + " ... " + list.getLast().getIndex() + " size: " + list.size());
      for (Log log : list) {
        if (reads.putIfAbsent(log.getIndex(), log) != null) {
          throw new RuntimeException("Duplicate read index!");
        }
      }
    }
  }
}
