package org.deephacks.logbuffers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The actual process that watch the log buffer for new logs.
 *
 * If scheduled forwarding is used, a single-threaded executor of the underlying log buffer
 * will be reused for every tail instance.
 *
 * This process consumes all logs of any type.
 */
public class LogBufferTail {
    private LogBuffer logBuffer;
    private Index readIndex;
    private Tail<Log> tail;

    public LogBufferTail(LogBuffer logBuffer, Tail<Log> tail) throws IOException {
        this.logBuffer = logBuffer;
        this.readIndex = new Index(logBuffer.getBasePath() + "/" + tail.getClass().getName());
        this.tail = tail;
    }

    /**
     * Push the index forward if logs are processed successfully by the tail.
     *
     * @throws IOException
     */
    public void forward() throws IOException {
        long currentWriteIndex = logBuffer.getIndex();
        long currentReadIndex = readIndex.getIndex();
        List<Log> messages = logBuffer.select(currentReadIndex, currentWriteIndex);
        tail.process(messages);
        // only write the read index if tail was successful
        readIndex.writeIndex(currentWriteIndex);
    }

    /**
     * Forwards the log processing periodically by notifying the tail each round.
     *
     * @param delay the delay between the termination of one execution and the commencement of the next.
     * @param unit time unit of the delay parameter.
     */
    public void forwardWithFixedDelay(int delay, TimeUnit unit) {
        this.logBuffer.getCachedExecutor().scheduleWithFixedDelay(new TailSchedule(this), 0, delay, unit);
    }

    public void shutdown() {
        logBuffer.getCachedExecutor().shutdown();
    }

    private static final class TailSchedule implements Runnable {
        private LogBufferTail tailer;

        public TailSchedule(LogBufferTail tailer) {
            this.tailer = tailer;
        }

        @Override
        public void run() {
            try {
                tailer.forward();
            } catch (Exception e) {
                // ignore for now
            }
        }
    }
}
