package org.deephacks.logbuffers.json;

import org.deephacks.logbuffers.RawLog;
import org.deephacks.logbuffers.LogBuffer;
import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.LogUtil;
import org.deephacks.logbuffers.Logs;
import org.deephacks.logbuffers.Tail;
import org.deephacks.logbuffers.json.JacksonSerializer.A;
import org.deephacks.logbuffers.json.JacksonSerializer.B;
import org.deephacks.logbuffers.json.JacksonSerializer.TailA;
import org.deephacks.logbuffers.json.JacksonSerializer.TailB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.deephacks.logbuffers.json.JacksonSerializer.randomA;
import static org.deephacks.logbuffers.json.JacksonSerializer.randomB;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class JacksonLogBufferTest {
  LogBuffer logBuffer;

  TailA tailA;
  TailB tailB;

  A a1 = randomA(1);
  A a2 = randomA(2);

  B b1 = randomB(1);
  B b2 = randomB(2);

  @Before
  public void before() throws IOException {
    tailA = new TailA();
    tailB = new TailB();
    logBuffer = new Builder()
            .basePath(LogUtil.tmpDir())
            .addSerializer(new JacksonSerializer())
            .build();
  }

  @After
  public void after() throws IOException {
    logBuffer.cancel(TailA.class);
    logBuffer.cancel(TailB.class);
    logBuffer.close();
  }

  @Test
  public void test_write_read_one_type() throws IOException {
    // one log
    logBuffer.write(a1);
    List<A> select = logBuffer.select(A.class, 0).get();
    assertThat(select.get(0), is(a1));

    // write another
    logBuffer.write(a2);
    select = logBuffer.select(A.class, 0).get();
    assertThat(select.size(), is(2));
    assertThat(select.get(0), is(a1));
    assertThat(select.get(1), is(a2));

    // forward index past first log
    select = logBuffer.select(A.class, 1).get();
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(a2));
  }


  @Test
  public void test_write_read_type_isolation() throws IOException {
    // A log
    logBuffer.write(a1);
    List<A> selectA = logBuffer.select(A.class, 0).get();
    assertThat(selectA.size(), is(1));
    assertThat(selectA.get(0), is(a1));

    // B log
    logBuffer.write(b1);
    List<B> selectB = logBuffer.select(B.class, 0).get();
    assertThat(selectB.size(), is(1));
    assertThat(selectB.get(0), is(b1));

    // second A log - check that B is not in the set
    logBuffer.write(a2);
    selectA = logBuffer.select(A.class, 0).get();
    assertThat(selectA.size(), is(2));
    assertThat(selectA.get(0), is(a1));
    assertThat(selectA.get(1), is(a2));

    // second B log - check that A is not in the set
    logBuffer.write(b2);
    selectB = logBuffer.select(B.class, 0).get();
    assertThat(selectB.size(), is(2));
    assertThat(selectB.get(0), is(b1));
    assertThat(selectB.get(1), is(b2));

    // select all raw logs
    List<RawLog> select = logBuffer.select(0);
    assertThat(select.size(), is(4));
  }

  @Test
  public void test_write_read_period() throws Exception {
    long t1 = timestamp();
    RawLog al1 = logBuffer.write(a1);

    long t2 = timestamp();
    RawLog bl1 = logBuffer.write(b1);

    long t3 = timestamp();
    RawLog al2 = logBuffer.write(a2);

    long t4 = timestamp();
    RawLog bl2 = logBuffer.write(b2);

    long t5 = timestamp();

    // check select outside index
    List<A> a = logBuffer.select(A.class, 100, 100).get();
    assertThat(a.size(), is(0));
    List<B> b = logBuffer.select(B.class, 100, 1000).get();
    assertThat(a.size(), is(0));
    assertThat(b.size(), is(0));

    a = logBuffer.selectBackward(A.class, Long.MAX_VALUE - 100000, Long.MAX_VALUE).get();

    // select a1 exactly
    a = logBuffer.selectBackward(A.class, al1.getTimestamp(), al1.getTimestamp()).get();
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a1));

    // select a2 exactly
    a = logBuffer.selectBackward(A.class, al2.getTimestamp(), al2.getTimestamp()).get();
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a2));

    // A selecting (a1) b1 a2 b2
    a = logBuffer.selectBackward(A.class, t1, t3).get();
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a1));

    // A selecting (a1 b1) a2 b2
    a = logBuffer.selectBackward(A.class, t1, t3).get();
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a1));

    // A selecting (a1 b1 a2 b2)
    a = logBuffer.selectBackward(A.class, t1, t5).get();
    assertThat(a.size(), is(2));
    assertThat(a.get(0), is(a1));
    assertThat(a.get(1), is(a2));

    // select b1 exactly
    b = logBuffer.selectBackward(B.class, bl1.getTimestamp(), bl1.getTimestamp()).get();
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b1));

    // select b2 exactly
    b = logBuffer.selectBackward(B.class, bl2.getTimestamp(), bl2.getTimestamp()).get();
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b2));

    // B selecting a1 (b1) a2 b2
    b = logBuffer.selectBackward(B.class, t2, t3).get();
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b1));

    // B selecting a1 (b1 a2) b2
    b = logBuffer.selectBackward(B.class, t2, t4).get();
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b1));

    // B selecting (a1 b1 a2 b2)
    b = logBuffer.selectBackward(B.class, t1, t5).get();
    assertThat(b.size(), is(2));
    assertThat(b.get(0), is(b1));
    assertThat(b.get(1), is(b2));
  }


  @Test
  public void test_manual_forward() throws IOException {
    // one log
    logBuffer.write(a1);
    logBuffer.forward(tailA);
    assertThat(tailA.logs.size(), is(1));
    assertThat(tailA.logs.get(0), is(a1));
    assertThat(tailB.logs.size(), is(0));

    // write another
    logBuffer.write(b1);
    logBuffer.forward(tailB);
    logBuffer.forward(tailA);
    assertThat(tailA.logs.size(), is(1));
    assertThat(tailA.logs.get(0), is(a1));
    assertThat(tailB.logs.size(), is(1));
    assertThat(tailB.logs.get(0), is(b1));

    // write multiple
    logBuffer.write(a1);
    logBuffer.write(b1);
    logBuffer.write(a2);
    logBuffer.write(b2);

    // only forward A
    logBuffer.forward(tailA);
    assertThat(tailA.logs.size(), is(3));
    assertThat(tailA.logs.get(1), is(a1));
    assertThat(tailA.logs.get(2), is(a2));
    assertThat(tailB.logs.size(), is(1));
    assertThat(tailB.logs.get(0), is(b1));

    // then B
    logBuffer.forward(tailB);
    assertThat(tailA.logs.size(), is(3));
    assertThat(tailB.logs.size(), is(3));
    assertThat(tailA.logs.get(1), is(a1));
    assertThat(tailA.logs.get(2), is(a2));
    assertThat(tailB.logs.get(1), is(b1));
    assertThat(tailB.logs.get(2), is(b2));

    assertThat(logBuffer.select(0).size(), is(6));
  }


  @Test
  public void test_scheduled_forward() throws Exception {

    logBuffer.forwardWithFixedDelay(tailA, 500, TimeUnit.MILLISECONDS);
    logBuffer.forwardWithFixedDelay(tailB, 500, TimeUnit.MILLISECONDS);

    // one A log
    logBuffer.write(a1);
    Thread.sleep(600);
    assertThat(tailA.logs.size(), is(1));
    assertThat(tailA.logs.get(0), is(a1));
    assertThat(tailB.logs.size(), is(0));

    // one B log
    logBuffer.write(b1);
    Thread.sleep(600);
    assertThat(tailA.logs.size(), is(1));
    assertThat(tailA.logs.get(0), is(a1));
    assertThat(tailB.logs.size(), is(1));
    assertThat(tailB.logs.get(0), is(b1));

    assertThat(logBuffer.select(0).size(), is(2));
  }

  @Test
  public void test_forward_failure() throws Exception {
    final List<A> result = new ArrayList<>();
    Tail failTail = new Tail<A>() {

      @Override
      public void process(Logs<A> logs) {
        result.addAll(logs.get());
        throw new IllegalArgumentException();
      }

    };
    logBuffer.write(a1);
    logBuffer.write(a2);
    try {
      logBuffer.forward(failTail);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    assertThat(result.size(), is(2));
    assertThat(result.get(0), is(a1));
    assertThat(result.get(1), is(a2));

    try {
      logBuffer.forward(failTail);
    } catch (IllegalArgumentException e) {
      // ignore
    }

    assertThat(result.size(), is(4));
    assertThat(result.get(0), is(a1));
    assertThat(result.get(1), is(a2));
    assertThat(result.get(2), is(a1));
    assertThat(result.get(3), is(a2));
  }

  @Test
  public void write_raw_logs_to_object_serializer() throws IOException {
    logBuffer.write(new byte[] {1});
    Logs<A> logs = logBuffer.selectBackward(A.class, 0, System.currentTimeMillis());
    assertThat(logs.size(), is(0));
  }

  long timestamp() throws InterruptedException {
    long time = System.currentTimeMillis();
    Thread.sleep(10);
    return time;
  }
}
