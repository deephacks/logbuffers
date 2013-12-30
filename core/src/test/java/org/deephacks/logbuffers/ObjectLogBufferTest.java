package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.LogUtil.A;
import org.deephacks.logbuffers.LogUtil.B;
import org.deephacks.logbuffers.json.JacksonSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ObjectLogBufferTest {
  LogBuffer logBuffer;

  A a1 = LogUtil.randomA(1);
  A a2 = LogUtil.randomA(2);

  B b1 = LogUtil.randomB(1);
  B b2 = LogUtil.randomB(2);

  @Before
  public void before() throws IOException {
    logBuffer = new Builder()
            .basePath(LogUtil.tmpDir())
            .addSerializer(new JacksonSerializer())
            .build();
  }

  @After
  public void after() throws IOException {
    logBuffer.close();
  }

  @Test
  public void test_write_read_one_type() throws IOException {
    // one log
    logBuffer.write(a1);
    List<A> select = logBuffer.select(A.class, 0);
    assertThat(select.get(0), is(a1));

    // write another
    logBuffer.write(a2);
    select = logBuffer.select(A.class, 0);
    assertThat(select.size(), is(2));
    assertThat(select.get(0), is(a1));
    assertThat(select.get(1), is(a2));

    // forward index past first log
    select = logBuffer.select(A.class, 1);
    assertThat(select.size(), is(1));
    assertThat(select.get(0), is(a2));
  }


  @Test
  public void test_write_read_type_isolation() throws IOException {
    // A log
    logBuffer.write(a1);
    List<A> selectA = logBuffer.select(A.class, 0);
    assertThat(selectA.size(), is(1));
    assertThat(selectA.get(0), is(a1));

    // B log
    logBuffer.write(b1);
    List<B> selectB = logBuffer.select(B.class, 0);
    assertThat(selectB.size(), is(1));
    assertThat(selectB.get(0), is(b1));

    // second A log - check that B is not in the set
    logBuffer.write(a2);
    selectA = logBuffer.select(A.class, 0);
    assertThat(selectA.size(), is(2));
    assertThat(selectA.get(0), is(a1));
    assertThat(selectA.get(1), is(a2));

    // second B log - check that A is not in the set
    logBuffer.write(b2);
    selectB = logBuffer.select(B.class, 0);
    assertThat(selectB.size(), is(2));
    assertThat(selectB.get(0), is(b1));
    assertThat(selectB.get(1), is(b2));

    // select all raw logs
    List<Log> select = logBuffer.select(0);
    assertThat(select.size(), is(4));
  }

  @Test
  public void test_write_read_period() throws Exception {
    long t1 = timestamp();
    Log al1 = logBuffer.write(a1);

    long t2 = timestamp();
    Log bl1 = logBuffer.write(b1);

    long t3 = timestamp();
    Log al2 = logBuffer.write(a2);

    long t4 = timestamp();
    Log bl2 = logBuffer.write(b2);

    long t5 = timestamp();

    // check select outside index
    List<A> a = logBuffer.select(A.class, 100, 100);
    assertThat(a.size(), is(0));
    List<B> b = logBuffer.select(B.class, 100, 1000);
    assertThat(a.size(), is(0));
    assertThat(b.size(), is(0));

    // select a1 exactly
    a = logBuffer.selectPeriod(A.class, al1.getTimestamp(), al1.getTimestamp());
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a1));

    // select a2 exactly
    a = logBuffer.selectPeriod(A.class, al2.getTimestamp(), al2.getTimestamp());
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a2));

    // A selecting (a1) b1 a2 b2
    a = logBuffer.selectPeriod(A.class, t1, t3);
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a1));

    // A selecting (a1 b1) a2 b2
    a = logBuffer.selectPeriod(A.class, t1, t3);
    assertThat(a.size(), is(1));
    assertThat(a.get(0), is(a1));

    // A selecting (a1 b1 a2 b2)
    a = logBuffer.selectPeriod(A.class, t1, t5);
    assertThat(a.size(), is(2));
    assertThat(a.get(0), is(a1));
    assertThat(a.get(1), is(a2));

    // select b1 exactly
    b = logBuffer.selectPeriod(B.class, bl1.getTimestamp(), bl1.getTimestamp());
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b1));

    // select b2 exactly
    b = logBuffer.selectPeriod(B.class, bl2.getTimestamp(), bl2.getTimestamp());
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b2));

    // B selecting a1 (b1) a2 b2
    b = logBuffer.selectPeriod(B.class, t2, t3);
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b1));

    // B selecting a1 (b1 a2) b2
    b = logBuffer.selectPeriod(B.class, t2, t4);
    assertThat(b.size(), is(1));
    assertThat(b.get(0), is(b1));

    // B selecting (a1 b1 a2 b2)
    b = logBuffer.selectPeriod(B.class, t1, t5);
    assertThat(b.size(), is(2));
    assertThat(b.get(0), is(b1));
    assertThat(b.get(1), is(b2));
  }

  long timestamp() throws InterruptedException {
    long time = System.currentTimeMillis();
    Thread.sleep(10);
    return time;
  }
}
