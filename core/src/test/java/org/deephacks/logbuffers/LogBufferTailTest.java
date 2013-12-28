package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.util.TestUtil;
import org.deephacks.logbuffers.util.TestUtil.TailLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class LogBufferTailTest {
    LogBuffer logBuffer;
    TailLog tail;

    Log log1 = TestUtil.randomLog(1);
    Log log2 = TestUtil.randomLog(2);

    @Before
    public void before() throws IOException {
        tail = new TailLog();
        logBuffer = new Builder().basePath(TestUtil.tmpDir()).build();
    }

    @After
    public void after() throws IOException {
        logBuffer.cancel(TailLog.class);
        logBuffer.close();
    }

    @Test
    public void test_manual_forward() throws IOException {
        // one log
        log1 = logBuffer.write(log1);
        logBuffer.forward(tail);
        assertThat(tail.logs.size(), is(1));
        assertThat(tail.logs.get(0), is(log1));

        // write another
        log2 = logBuffer.write(log2);
        logBuffer.forward(tail);
        assertThat(tail.logs.size(), is(2));
        assertThat(tail.logs.get(1), is(log2));

        // write multiple
        log1 = logBuffer.write(log1);
        log2 = logBuffer.write(log2);
        logBuffer.forward(tail);
        assertThat(tail.logs.size(), is(4));
        assertThat(tail.logs.get(2), is(log1));
        assertThat(tail.logs.get(3), is(log2));
    }


    @Test
    public void test_scheduled_forward() throws Exception {
        logBuffer.forwardWithFixedDelay(tail, 500, TimeUnit.MILLISECONDS);
        // one log
        log1 = logBuffer.write(log1);
        Thread.sleep(600);
        assertThat(tail.logs.size(), is(1));
        assertThat(tail.logs.get(0), is(log1));

        // write another
        log2 = logBuffer.write(log2);
        Thread.sleep(600);

        assertThat(tail.logs.size(), is(2));
        assertThat(tail.logs.get(1), is(log2));

        // write multiple
        log1 = logBuffer.write(log1);
        log2 = logBuffer.write(log2);
        Thread.sleep(600);

        assertThat(tail.logs.size(), is(4));
        assertThat(tail.logs.get(2), is(log1));
        assertThat(tail.logs.get(3), is(log2));
    }
}
