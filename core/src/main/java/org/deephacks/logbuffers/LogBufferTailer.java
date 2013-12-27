package org.deephacks.logbuffers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LogBufferTailer {
    private LogBuffer logBuffer;
    private Index readIndex;
    private Tail<Log> tail;

    public LogBufferTailer(LogBuffer logBuffer, Tail<Log> tail) throws IOException {
        this.logBuffer = logBuffer;
        this.readIndex = new Index(logBuffer.getBasePath() + "/" + tail.getName());
        this.tail = tail;
    }

    public void forward() throws IOException {
        long currentWriteIndex = logBuffer.getIndex();
        long currentReadIndex = readIndex.getIndex();
        List<Log> messages = logBuffer.select(currentReadIndex, currentWriteIndex);
        tail.process(messages);
        // only write the read index if tail was successful
        readIndex.writeIndex(currentWriteIndex);
    }

    public void forwardWithFixedDelay(int delay, TimeUnit unit) {
        this.logBuffer.getCachedExecutor().scheduleWithFixedDelay(new TailSchedule(this), 0, delay, unit);
    }

    private static final class TailSchedule implements Runnable {
        private LogBufferTailer tailer;

        public TailSchedule(LogBufferTailer tailer) {
            this.tailer = tailer;
        }

        @Override
        public void run() {
            try {
                tailer.forward();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
