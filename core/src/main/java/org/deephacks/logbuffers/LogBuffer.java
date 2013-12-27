package org.deephacks.logbuffers;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.RollingChronicle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LogBuffer {
    private ScheduledExecutorService cachedExecutor;
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
    private static final String DEFAULT_BASE_PATH = TMP_DIR + "/logbuffer";
    private Index index;
    private RollingChronicle rollingChronicle;
    private ExcerptAppender excerptAppender;
    private ExcerptTailer excerptTailer;
    private final Object readLock = new Object();
    private final Object writeLock = new Object();
    private String basePath;

    private LogBuffer(Builder builder) throws IOException {
        this.basePath = builder.basePath.or(DEFAULT_BASE_PATH);
        this.index = new Index(basePath + "/writer");
        this.rollingChronicle = new RollingChronicle(basePath, builder.config);
        this.excerptAppender = rollingChronicle.createAppender();
        this.excerptTailer = rollingChronicle.createTailer();
    }

    synchronized ScheduledExecutorService getCachedExecutor() {
        if (cachedExecutor == null) {
            cachedExecutor = Executors.newSingleThreadScheduledExecutor();
        }
        return cachedExecutor;
    }

    public void write(byte[] message) throws IOException {
        write(new Log(message));
    }

    public void write(String message) throws IOException {
        byte[] msg = message.getBytes(Charsets.UTF_8);
        write(new Log(msg));
    }

    public void write(Log log) throws IOException {
        synchronized (writeLock) {
            log.write(excerptAppender);
            index.getAndIncrement();
        }
    }

    public List<Log> select(long fromIndex) throws IOException {
        long writeIdx = index.getIndex();
        return select(fromIndex, writeIdx);
    }

    public List<Log> select(long fromIndex, long toIndex) throws IOException {
        synchronized (readLock) {
            List<Log> messages = new ArrayList<>();
            long read = fromIndex;
            while (read < toIndex) {
                messages.add(Log.read(excerptTailer, read++));
            }
            return messages;
        }
    }

    public String getBasePath() {
        return basePath;
    }

    public synchronized void close() throws IOException {
        excerptAppender.close();
        excerptTailer.close();
        rollingChronicle.close();
    }

    public long getIndex() throws IOException {
        return index.getIndex();
    }

    public static class Builder {
        private ChronicleConfig config = ChronicleConfig.SMALL.clone();
        private Optional<String> basePath = Optional.absent();

        public Builder() {
            config.indexFileExcerpts(256);
        }

        public Builder basePath(String basePath) {
            this.basePath = Optional.fromNullable(basePath);
            return this;
        }

        public Builder logFiles(int numLogFiles) {
            config.indexFileExcerpts(numLogFiles);
            return this;
        }

        public LogBuffer build() throws IOException {
            return new LogBuffer(this);
        }
    }
}
