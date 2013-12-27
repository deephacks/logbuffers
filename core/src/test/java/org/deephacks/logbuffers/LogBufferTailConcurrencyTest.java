package org.deephacks.logbuffers;

import com.google.common.base.Stopwatch;
import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.TestUtil.TailLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class LogBufferTailConcurrencyTest {
    LogBuffer logBuffer;
    LogBufferTail logBufferTail;
    TailLog tailLog;
    int numLogs = 1_000_000;

    @Before
    public void before() throws IOException {
        logBuffer = new Builder().logsPerFile(numLogs).basePath(TestUtil.tmpDir()).build();
        tailLog = new TailLog();
        logBufferTail = new LogBufferTail(logBuffer, tailLog);
    }

    @After
    public void after() throws IOException {
        logBuffer.close();
    }

    @Test
    public void test_concurrency() throws IOException, InterruptedException {

        ExecutorService executor = Executors.newCachedThreadPool();
        Stopwatch stopwatch = new Stopwatch().start();
        final CountDownLatch latch = new CountDownLatch(numLogs);
        for (int i = 0; i < numLogs; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        logBuffer.write(TestUtil.randomCachedItem());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        System.out.println("Wrote " + numLogs + " in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        stopwatch = new Stopwatch().start();
        logBufferTail.forward();
        System.out.println("Read " + numLogs + " in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        assertThat(tailLog.logs.size(), is(numLogs));
    }
}
