package org.deephacks.logbuffers;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.RollingChronicle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public final class LogBuffer {
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

    private ConcurrentHashMap<Tail<?>, LogBufferTail<?>> tails = new ConcurrentHashMap<>();

    private Serializers serializers;

    private LogBuffer(Builder builder) throws IOException {
        this.basePath = builder.basePath.or(DEFAULT_BASE_PATH);
        this.index = new Index(basePath + "/writer");
        this.rollingChronicle = new RollingChronicle(basePath, builder.config);
        this.excerptAppender = rollingChronicle.createAppender();
        this.excerptTailer = rollingChronicle.createTailer();
        this.serializers = builder.serializers;
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
    public Log write(byte[] log) throws IOException {
        return write(new Log(log));
    }

    /**
     * Write a new log as a string encoded in UTF-8.
     *
     * @param log data as a UTF-8 string.
     * @throws IOException
     */
    public Log write(String log) throws IOException {
        return write(new Log(log.getBytes(Charsets.UTF_8)));
    }

    /**
     * Write a new raw log object.
     *
     * @param log raw object log.
     * @throws IOException
     */
    public Log write(Log log) throws IOException {
        synchronized (writeLock) {
            log.write(excerptAppender);
            return new Log(log, index.getAndIncrement());
        }
    }

    /**
     * Write an object into the log buffer.
     *
     * @param object to be written.
     * @return the unique index assigned to the object.
     * @throws IOException
     */
    public long write(Object object) throws IOException {
        return write(object, System.currentTimeMillis());
    }

    /**
     * Write an object into the log buffer.
     *
     * @param object to be written
     * @param timestamp of the object used by queries
     * @return the unique index assigned to the object.
     * @throws IOException
     */
    public long write(Object object, long timestamp) throws IOException {
        Class<?> cls = object.getClass();
        ObjectLogSerializer serializer = serializers.getSerializer(cls);
        byte[] content = serializer.serialize(object);
        Log log = new Log(serializers.getType(cls), content, timestamp, -1);
        return write(log).getIndex();
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

    /**
     * Select a list of log objects for a given period of time with respect
     * to the timestamp of each log.
     *
     * @param fromTimeMs from (inclusive)
     * @param toTimeMs to (inclusive)
     * @return list of matching logs
     * @throws IOException
     */
    public List<Log> selectPeriod(long fromTimeMs, long toTimeMs) throws IOException {
        long writeIndex = index.getIndex();
        synchronized (readLock) {
            LinkedList<Log> messages = new LinkedList<>();
            long read = writeIndex - 1;
            Log log = Log.read(excerptTailer, read);
            while (log.getTimestamp() > toTimeMs) {
                log = Log.read(excerptTailer, read);
            }
            while (log.getTimestamp() >= fromTimeMs) {
                messages.addFirst(log);
                log = Log.read(excerptTailer, --read);
            }
            return messages;
        }
    }

    /**
     * Selects logs only of a specific type, all other types are filtered out.
     *
     * @see LogBuffer
     */
    public <T> List<T> select(Class<T> type, long fromIndex) throws IOException {
        return select(type, fromIndex, getIndex());
    }

    /**
     * Selects logs only of a specific type, all other types are filtered out.
     *
     * @see LogBuffer
     */
    public <T> List<T> select(Class<T> type, long fromIndex, long toIndex) throws IOException {
        List<Log> logs = select(fromIndex, toIndex);
        List<T> objects = new ArrayList<>();
        for (Log log : logs) {
            Object object = log;
            if (log.getType() != Log.DEFAULT_TYPE) {
                ObjectLogSerializer serializer = serializers.getSerializer(log.getType());
                object = serializer.deserialize(log.getContent(), log.getType());
            }
            if (type.isAssignableFrom(object.getClass())) {
                objects.add((T) object);
            }
        }
        return objects;
    }

    public <T> void forward(Tail<T> tail) throws IOException {
        LogBufferTail<T> tailBuffer = putIfAbsent(tail);
        tailBuffer.forward();
    }

    /**
     * @return directory where this log buffer is stored
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Close this log buffer.
     *
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if (cachedExecutor != null) {
            cachedExecutor.shutdown();
        }
        index.close();
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

    /**
     * Cancel the periodic tail task.
     *
     * @param mayInterruptIfRunning if the thread executing this
     * task should be interrupted; otherwise, in-progress tasks are allowed
     * to complete
     */
    public void cancel(Tail<?> tail, boolean mayInterruptIfRunning) throws IOException {
        LogBufferTail<?> logBufferTail = putIfAbsent(tail);
        logBufferTail.cancel(mayInterruptIfRunning);
    }

    /**
     * Forwards the log processing periodically by notifying the tail each round.
     *
     * @param delay the delay between the termination of one execution and the commencement of the next.
     * @param unit time unit of the delay parameter.
     */
    public void forwardWithFixedDelay(Tail<?> tail, int delay, TimeUnit unit) throws IOException {
        LogBufferTail<?> logBufferTail = putIfAbsent(tail);
        logBufferTail.forwardWithFixedDelay(delay, unit);
    }

    private <T> LogBufferTail<T> putIfAbsent(Tail<?> tail) throws IOException {
        LogBufferTail<?> logBufferTail = tails.putIfAbsent(tail, new LogBufferTail<>(this, tail));
        if (logBufferTail == null) {
            logBufferTail = tails.get(tail);
        }
        return (LogBufferTail<T>) logBufferTail;
    }

    public static class Builder {
        private ChronicleConfig config = ChronicleConfig.SMALL.clone();
        private Optional<String> basePath = Optional.absent();
        private Serializers serializers = new Serializers();

        public Builder() {
            config.indexFileExcerpts(Short.MAX_VALUE);
        }

        public Builder basePath(String basePath) {
            this.basePath = Optional.fromNullable(basePath);
            return this;
        }

        public Builder addSerializer(ObjectLogSerializer serializer) {
            serializers.addSerializer(serializer);
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
