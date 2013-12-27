package org.deephacks.logbuffers;


import com.google.common.base.Optional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class ObjectLogBuffer {
    private final LogBuffer logBuffer;
    /** */
    private ObjectLogSerializer serializer;


    public ObjectLogBuffer(LogBuffer logBuffer, ObjectLogSerializer serializer) {
        this.logBuffer = logBuffer;
        this.serializer = serializer;
    }

    public String getBasePath() {
        return logBuffer.getBasePath();
    }

    public long getIndex() throws IOException {
        return logBuffer.getIndex();
    }

    public ScheduledExecutorService getCachedExecutor() {
        return logBuffer.getCachedExecutor();
    }

    public void write(Object object) throws IOException {
        logBuffer.write(serializer.serialize(object));
    }

    public <T> List<T> select(Class<T> type, long fromIndex) throws IOException {
        return select(type, fromIndex, logBuffer.getIndex());
    }

    public <T> List<T> select(Class<T> type, long fromIndex, long toIndex) throws IOException {
        List<Log> logs = logBuffer.select(fromIndex, toIndex);
        List<T> objects = new ArrayList<>();
        for (Log log : logs) {
            Optional<T> optional = serializer.deserialize(log, type);
            if (optional.isPresent() && type.isAssignableFrom(optional.get().getClass())) {
                objects.add(optional.get());
            }
        }
        return objects;
    }
}
