package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.json.JacksonSerializer;
import org.deephacks.logbuffers.util.TestUtil;
import org.deephacks.logbuffers.util.TestUtil.A;
import org.deephacks.logbuffers.util.TestUtil.B;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ObjectLogBufferTest {
    ObjectLogBuffer objectLogBuffer;
    LogBuffer logBuffer;

    A a1 = TestUtil.randomA();
    A a2 = TestUtil.randomA();

    B b1 = TestUtil.randomB();
    B b2 = TestUtil.randomB();

    @Before
    public void before() throws IOException {
        logBuffer = new Builder().basePath(TestUtil.tmpDir()).build();
        objectLogBuffer = new ObjectLogBuffer(logBuffer, new JacksonSerializer());
    }

    @After
    public void after() throws IOException {
        objectLogBuffer.close();
    }

    @Test
    public void test_write_read_one_type() throws IOException {
        // one log
        objectLogBuffer.write(a1);
        List<A> select = objectLogBuffer.select(A.class, 0);
        assertThat(select.get(0), is(a1));

        // write another
        objectLogBuffer.write(a2);
        select = objectLogBuffer.select(A.class, 0);
        assertThat(select.size(), is(2));
        assertThat(select.get(0), is(a1));
        assertThat(select.get(1), is(a2));

        // forward index past first log
        select = objectLogBuffer.select(A.class, 1);
        assertThat(select.size(), is(1));
        assertThat(select.get(0), is(a2));
    }


    @Test
    public void test_write_read_type_isolation() throws IOException {
        // A log
        objectLogBuffer.write(a1);
        List<A> selectA = objectLogBuffer.select(A.class, 0);
        assertThat(selectA.size(), is(1));
        assertThat(selectA.get(0), is(a1));

        // B log
        objectLogBuffer.write(b1);
        List<B> selectB = objectLogBuffer.select(B.class, 0);
        assertThat(selectB.size(), is(1));
        assertThat(selectB.get(0), is(b1));

        // second A log - check that B is not in the set
        objectLogBuffer.write(a2);
        selectA = objectLogBuffer.select(A.class, 0);
        assertThat(selectA.size(), is(2));
        assertThat(selectA.get(0), is(a1));
        assertThat(selectA.get(1), is(a2));

        // second B log - check that A is not in the set
        objectLogBuffer.write(b2);
        selectB = objectLogBuffer.select(B.class, 0);
        assertThat(selectB.size(), is(2));
        assertThat(selectB.get(0), is(b1));
        assertThat(selectB.get(1), is(b2));

        // select all raw logs
        List<Log> select = logBuffer.select(0);
        assertThat(select.size(), is(4));
    }
}
