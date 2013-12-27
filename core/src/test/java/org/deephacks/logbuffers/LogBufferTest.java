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
    Log log1 = TestUtil.randomLog();
    Log log2 = TestUtil.randomLog();

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
        logBuffer.write(log1);
        List<Log> select = logBuffer.select(0);
        assertThat(select.get(0), is(log1));

        // write another
        logBuffer.write(log2);
        select = logBuffer.select(0);
        assertThat(select.size(), is(2));
        assertThat(select.get(0), is(log1));
        assertThat(select.get(1), is(log2));

        // forward index past first log
        select = logBuffer.select(1);
        assertThat(select.size(), is(1));
        assertThat(select.get(0), is(log2));

    }
}
