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

/**
 * A log buffer persist data items sequentially on local disk.
 *
 * Every written log is assigned a unique index identifier that works like an sequential offset.
 * Indexes are used by readers to select one or more logs.
 *
 * A logical log buffer is divided into a set of files in a configurable directory.
 * New files are created when the capacity of the current file is reached. Files
 * are not deleted at the moment.
 *
 * The physical separation of a log buffer is an implementation detail that the user
 * does not need to care about.
 */
public class LogBuffer {
    /** system tmp dir*/
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /** default path used by log files if not specified */
    private static final String DEFAULT_BASE_PATH = TMP_DIR + "/logbuffer";

    /** optional executor used only by scheduled tailing */
    private ScheduledExecutorService cachedExecutor;

    /** the current write index */
    private Index index;

    /** rolling chronicle, log buffer files are never removed ATM  */
    private RollingChronicle rollingChronicle;

    /** log writer */
    private ExcerptAppender excerptAppender;

    /** log reader */
    private ExcerptTailer excerptTailer;

    /** lock used when reading */
    private final Object readLock = new Object();

    /** locked used when writing */
    private final Object writeLock = new Object();

    /** path where log buffer files are stored */
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

    /**
     * Write a new raw binary log.
     *
     * @param log data.
     * @throws IOException
     */
    public void write(byte[] log) throws IOException {
        write(new Log(log));
    }

    /**
     * Write a new log as a string encoded in UTF-8.
     *
     * @param log data as a UTF-8 string.
     * @throws IOException
     */
    public void write(String log) throws IOException {
        write(new Log(log.getBytes(Charsets.UTF_8)));
    }

    /**
     * Write a new raw log object.
     *
     * @param log raw object log.
     * @throws IOException
     */
    public void write(Log log) throws IOException {
        synchronized (writeLock) {
            log.write(excerptAppender);
            index.getAndIncrement();
        }
    }

    /**
     * Select a list of log objects from a specific index up until the
     * most recent log written.
     *
     * @param fromIndex index to read from.
     * @return A list of raw object logs.
     * @throws IOException
     */
    public List<Log> select(long fromIndex) throws IOException {
        long writeIdx = index.getIndex();
        return select(fromIndex, writeIdx);
    }


    /**
     * Select a list of log objects from a specific index up until a
     * provided index.
     *
     * @param fromIndex index to read from.
     * @param toIndex index to read up until.
     * @return A list of raw object logs.
     * @throws IOException
     */
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

    /**
     * Close this log buffer.
     *
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        excerptAppender.close();
        excerptTailer.close();
        rollingChronicle.close();
    }

    /**
     * @return the current write index.
     * @throws IOException
     */
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

        public Builder logsPerFile(int logsPerFile) {
            config.indexFileExcerpts(logsPerFile);
            return this;
        }

        public LogBuffer build() throws IOException {
            return new LogBuffer(this);
        }
    }
}
