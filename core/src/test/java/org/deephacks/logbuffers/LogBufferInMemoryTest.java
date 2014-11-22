package org.deephacks.logbuffers;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LogBufferInMemoryTest {

  @Test
  public void shouldReturnEmptyForEmptyDirs() throws IOException {
    LogBuffer logBuffer = LogUtil.empty();
    Query query = Query.closedTime(0, Long.MAX_VALUE);
    assertThat(logBuffer.find(query).stream().count(), is(0L));
  }

  @Test
  public void shouldFindTime() throws IOException {
    long now = System.currentTimeMillis();
    LogBuffer buffer = LogUtil.newBuilder()
      .tick(now)
      .add("1")
      .add("2")
      .build();
    Query query = Query.closedTime(now, now);
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getUtf8(), is("1"));
    assertThat(list.get(1).getUtf8(), is("2"));
  }

  @Test
  public void shouldFindTimeSameTimestamp() throws IOException {
    long now = System.currentTimeMillis();
    LogUtil.Builder builder = LogUtil.newBuilder().tick(now);
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 10000; i++) {
        builder.add("1");
      }
      builder.tick(1);
    }
    LogBuffer buffer = builder.build();
    AtomicInteger round = new AtomicInteger();
    for (int i = 0; i < 9; i++) {
      round.incrementAndGet();
      Query query = Query.closedTime(now + round.get(), now + round.get());
      AtomicLong count = new AtomicLong();
      buffer.find(query).stream().forEach(l -> {
        if (l.getTimestamp() != now + round.get()) {
          throw new IllegalStateException("" + round.get());
        }
        count.incrementAndGet();
      });

      assertThat(count.get(), is(10000L));
    }
  }

  @Test
  public void shouldFindTimeOverRolling() throws IOException {
    LogBuffer buffer = LogUtil.newBuilder()
      .tick(0)
      .paddedEntry()
      .paddedEntry()
      .paddedEntry()
      .add("1")
      .add("2")
      .tick(5000)
      .paddedEntry()
      .paddedEntry()
      .paddedEntry()
      .add("3")
      .paddedEntry()
      .tick(10000)
      .add("4")
      .tick(15000)
      .add("5")
      .paddedEntry()
      .build();
    Query query = Query.closedTime(0, 1000000);
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(5));
    assertThat(list.get(0).getUtf8(), is("1"));
    assertThat(list.get(1).getUtf8(), is("2"));
    assertThat(list.get(2).getUtf8(), is("3"));
    assertThat(list.get(3).getUtf8(), is("4"));
    assertThat(list.get(4).getUtf8(), is("5"));
  }

  @Test
  public void shouldFindIndexOverRolling() throws IOException {
    LogBuffer buffer = LogUtil.newBuilder()
      .paddedEntry()
      .tick(0)
      .add("1")
      .tick(5000)
      .add("2")
      .tick(10000)
      .paddedEntry()
      .add("3")
      .paddedEntry()
      .tick(15000)
      .add("4")
      .paddedEntry()
      .build();
    Query query = Query.closedIndex(0, 100000000000L);
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(4));
    assertThat(list.get(0).getUtf8(), is("1"));
    assertThat(list.get(1).getUtf8(), is("2"));
    assertThat(list.get(2).getUtf8(), is("3"));
    assertThat(list.get(3).getUtf8(), is("4"));
  }

  @Test
  public void shouldNotFindTime() throws IOException {
    long now = System.currentTimeMillis();
    LogBuffer buffer = LogUtil.newBuilder()
      .tick(now)
      .add("1")
      .add("2")
      .build();
    Query query = Query.atLeastTime(now + 1);
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(0));
    query = Query.atMostTime(now - 1);
    list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(0));
  }

  @Test
  public void shouldFindIndex() throws IOException {
    long now = System.currentTimeMillis();
    LogBuffer buffer = LogUtil.newBuilder()
      .tick(now)
      .add("1")
      .add("2")
      .build();
    Query query = Query.closedIndex(LogUtil.logs.firstKey(), LogUtil.logs.lastKey());
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getUtf8(), is("1"));
    assertThat(list.get(1).getUtf8(), is("2"));
  }

  @Test
  public void shouldNotFindIndex() throws IOException {
    long now = System.currentTimeMillis();
    LogBuffer buffer = LogUtil.newBuilder()
      .tick(now)
      .add("1")
      .add("2")
      .build();
    Query query = Query.atMostIndex(LogUtil.logs.firstKey() - 1);
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(0));
    query = Query.atLeastIndex(LogUtil.logs.lastKey() + 1);
    list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(0));
  }

  @Test
  public void shouldIgnorePaddedEntries() throws IOException {
    long now = System.currentTimeMillis();
    LogBuffer buffer = LogUtil.newBuilder()
      .tick(now)
      .paddedEntry()
      .add("1")
      .paddedEntry()
      .add("2")
      .paddedEntry()
      .paddedEntry()
      .add("3")
      .paddedEntry()
      .build();
    Query query = Query.closedTime(now, now);
    List<Log> list = buffer.find(query).toLinkedList();
    assertThat(list.size(), is(3));
    assertThat(list.get(0).getUtf8(), is("1"));
    assertThat(list.get(1).getUtf8(), is("2"));
    assertThat(list.get(2).getUtf8(), is("3"));
  }


}
