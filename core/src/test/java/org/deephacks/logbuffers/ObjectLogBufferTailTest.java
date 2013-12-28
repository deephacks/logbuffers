package org.deephacks.logbuffers;

import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.json.JacksonSerializer;
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

        logBuffer = new Builder().basePath(TestUtil.tmpDir())
                .addSerializer(new JacksonSerializer())
                .build();
    }

    @After
    public void after() throws IOException {
        logBuffer.close();
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
            public void process(List<A> logs) {
                result.addAll(logs);
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
}
