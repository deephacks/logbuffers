package org.deephacks.logbuffers;

import com.google.common.base.Optional;

/**
 * Serialize objects to and from raw object log format.
 */
public interface ObjectLogSerializer {
    /**
     * Called when a new object is written to the object log buffer that
     * this serializer is attached to.
     *
     * @param object that is written.
     * @return a raw object log.
     */
    public Log serialize(Object object);

    /**
     * Called when the object log buffer is queried with an index range that covers a specific log.
     *
     * Note that logs not understood by this specific serializer might also be provided, hence the
     * Optional return object.
     *
     * @param log that was queried.
     * @param type object type.
     * @param <T> generic type.
     * @return Present if this serializer know how to serialize the log object.
     */
    public <T> Optional<T> deserialize(Log log, Class<T> type);
}
