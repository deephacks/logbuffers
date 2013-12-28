package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.util.TestUtil;
import org.deephacks.logbuffers.util.TestUtil.A;
import org.deephacks.logbuffers.util.TestUtil.B;
import org.deephacks.logbuffers.util.TestUtil.TailA;
import org.deephacks.logbuffers.util.TestUtil.TailB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ObjectLogBufferTailTest {
    LogBuffer logBuffer;
    ObjectLogBuffer objectLogBuffer;
    ObjectLogBufferTail<A> bufferTailA;
    ObjectLogBufferTail<B> bufferTailB;

    TailA tailA;
    TailB tailB;

    A a1 = TestUtil.randomA();
    A a2 = TestUtil.randomA();

    B b1 = TestUtil.randomB();
    B b2 = TestUtil.randomB();

    @Before
    public void before() throws IOException {
        tailA = new TailA();
        tailB = new TailB();

        logBuffer = new Builder().basePath(TestUtil.tmpDir()).build();
        objectLogBuffer = new ObjectLogBuffer(logBuffer, TestUtil.getSerializer());

        bufferTailA = new ObjectLogBufferTail<>(objectLogBuffer, tailA);
        bufferTailB = new ObjectLogBufferTail<>(objectLogBuffer, tailB);
    }

    @After
    public void after() throws IOException {
        logBuffer.close();
    }

    @Test
    public void test_manual_forward() throws IOException {
        // one log
        objectLogBuffer.write(a1);
        bufferTailA.forward();
        assertThat(tailA.logs.size(), is(1));
        assertThat(tailA.logs.get(0), is(a1));
        assertThat(tailB.logs.size(), is(0));

        // write another
        objectLogBuffer.write(b1);
        bufferTailB.forward();
        bufferTailA.forward();
        assertThat(tailA.logs.size(), is(1));
        assertThat(tailA.logs.get(0), is(a1));
        assertThat(tailB.logs.size(), is(1));
        assertThat(tailB.logs.get(0), is(b1));

        // write multiple
        objectLogBuffer.write(a1);
        objectLogBuffer.write(b1);
        objectLogBuffer.write(a2);
        objectLogBuffer.write(b2);

        // only forward A
        bufferTailA.forward();
        assertThat(tailA.logs.size(), is(3));
        assertThat(tailA.logs.get(1), is(a1));
        assertThat(tailA.logs.get(2), is(a2));
        assertThat(tailB.logs.size(), is(1));
        assertThat(tailB.logs.get(0), is(b1));

        // then B
        bufferTailB.forward();
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
        bufferTailA.forwardWithFixedDelay(500, TimeUnit.MILLISECONDS);
        bufferTailB.forwardWithFixedDelay(500, TimeUnit.MILLISECONDS);

        // one A log
        objectLogBuffer.write(a1);
        Thread.sleep(600);
        assertThat(tailA.logs.size(), is(1));
        assertThat(tailA.logs.get(0), is(a1));
        assertThat(tailB.logs.size(), is(0));

        // one B log
        objectLogBuffer.write(b1);
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
            public void process(List<A> logs) {
                result.addAll(logs);
                throw new IllegalArgumentException();
            }

        };
        ObjectLogBufferTail failBufferTail = new ObjectLogBufferTail<>(objectLogBuffer, failTail);
        objectLogBuffer.write(a1);
        objectLogBuffer.write(a2);
        try {
            failBufferTail.forward();
        } catch (IllegalArgumentException e) {
            // ignore
        }
        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(a1));
        assertThat(result.get(1), is(a2));

        try {
            failBufferTail.forward();
        } catch (IllegalArgumentException e) {
            // ignore
        }

        assertThat(result.size(), is(4));
        assertThat(result.get(0), is(a1));
        assertThat(result.get(1), is(a2));
        assertThat(result.get(2), is(a1));
        assertThat(result.get(3), is(a2));

    }
}
