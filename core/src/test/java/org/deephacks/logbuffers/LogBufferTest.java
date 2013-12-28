package org.deephacks.logbuffers;


import org.deephacks.logbuffers.LogBuffer.Builder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class LogBufferTest {
    LogBuffer logBuffer;
    Log log1 = TestUtil.randomLog(1);
    Log log2 = TestUtil.randomLog(2);
    Log log3 = TestUtil.randomLog(3);
    Log log4 = TestUtil.randomLog(4);
    Log log5 = TestUtil.randomLog(5);

    @Before
    public void before() throws IOException {
        logBuffer = new Builder().basePath(TestUtil.tmpDir()).build();
    }

    @After
    public void after() throws IOException {
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

        List<Log> select = logBuffer.selectPeriod(3, 5);
        assertThat(select.size(), is(3));
        assertThat(select.get(0), is(log3));
        assertThat(select.get(1), is(log4));
        assertThat(select.get(2), is(log5));
    }
}
