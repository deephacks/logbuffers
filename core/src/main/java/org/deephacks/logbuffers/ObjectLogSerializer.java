package org.deephacks.logbuffers;

import com.google.common.base.Optional;

public interface ObjectLogSerializer {

    public Log serialize(Object object);

    public <T> Optional<T> deserialize(Log log, Class<T> type);
}
