package org.deephacks.logbuffers;


import com.google.common.base.Optional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A log buffer persist log objects sequentially on local disk.
 *
 * Identical to a raw log buffer except that object serialization is supported
 * using json, protobuf, avro or similar.
 *
 */
public final class ObjectLogBuffer {
    /** the raw underlying log buffer */
    private final LogBuffer logBuffer;

    /** serialization mechanism for all objects flowing in and out of the log buffer */
    private ObjectLogSerializer serializer;

    /**
     * @param logBuffer the raw underlying log buffer
     * @param serializer serialization mechanism for all objects flowing in and out of the log buffer
     */
    public ObjectLogBuffer(LogBuffer logBuffer, ObjectLogSerializer serializer) {
        this.logBuffer = logBuffer;
        this.serializer = serializer;
    }

    /**
     * @see LogBuffer
     */
    public String getBasePath() {
        return logBuffer.getBasePath();
    }

    /**
     * @see LogBuffer
     */
    public long getIndex() throws IOException {
        return logBuffer.getIndex();
    }

    /**
     * @return the executor that govern tails on this log buffer
     */
    public ScheduledExecutorService getCachedExecutor() {
        return logBuffer.getCachedExecutor();
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
        Optional<Long> type = serializer.getType(object.getClass());
        if (!type.isPresent()) {
            throw new IllegalArgumentException("No mapping found for " + object.getClass());
        }
        byte[] content = serializer.serialize(object);
        Log log = new Log(type.get(), content, timestamp, -1);
        return logBuffer.write(log).getIndex();
    }

    /**
     * Selects logs only of a specific type, all other types are filtered out.
     *
     * @see LogBuffer
     */
    public <T> List<T> select(Class<T> type, long fromIndex) throws IOException {
        return select(type, fromIndex, logBuffer.getIndex());
    }

    /**
     * Selects logs only of a specific type, all other types are filtered out.
     *
     * @see LogBuffer
     */
    public <T> List<T> select(Class<T> type, long fromIndex, long toIndex) throws IOException {
        List<Log> logs = logBuffer.select(fromIndex, toIndex);
        List<T> objects = new ArrayList<>();
        for (Log log : logs) {
            Optional<T> optional = serializer.deserialize(log.getContent(), log.getType());
            if (optional.isPresent() && type.isAssignableFrom(optional.get().getClass())) {
                objects.add(optional.get());
            }
        }
        return objects;
    }

    /**
     * Close the log buffer.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        logBuffer.close();
    }
}
