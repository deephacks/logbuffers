package org.deephacks.logbuffers;


import javax.lang.model.type.TypeVariable;
import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The actual process that watch a object log buffer for new logs of a specific type.
 *
 * If scheduled forwarding is used, a single-threaded executor of the underlying log buffer
 * will be reused for every tail instance.
 *
 * @param <T> type of logs to process.
 */
public final class ObjectLogBufferTail<T> {
    private ObjectLogBuffer logBuffer;
    private Index readIndex;
    private Tail<T> tail;
    private Class<T> type;

    public ObjectLogBufferTail(ObjectLogBuffer logBuffer, Tail<T> tail) throws IOException {
        this.logBuffer = logBuffer;
        this.readIndex = new Index(logBuffer.getBasePath() + "/" + tail.getClass().getName());
        this.tail = tail;
        this.type = (Class<T>) getParameterizedType(tail.getClass(), Tail.class).get(0);
    }

    /**
     * Push the index forward if logs are processed successfully by the tail.
     *
     * @throws IOException
     */
    public void forward() throws IOException {
        long currentWriteIndex = logBuffer.getIndex();
        long currentReadIndex = readIndex.getIndex();
        List<T> messages = logBuffer.select(type, currentReadIndex, currentWriteIndex);
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

    private static final class TailSchedule implements Runnable {
        private ObjectLogBufferTail tailer;

        public TailSchedule(ObjectLogBufferTail tailer) {
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

    private List<Class<?>> getParameterizedType(final Class<?> ownerClass, Class<?> genericSuperClass) {
        Type[] types = null;
        if (genericSuperClass.isInterface()) {
            types = ownerClass.getGenericInterfaces();
        } else {
            types = new Type[] { ownerClass.getGenericSuperclass() };
        }

        List<Class<?>> classes = new ArrayList<>();
        for (Type type : types) {
            if (!ParameterizedType.class.isAssignableFrom(type.getClass())) {
                return new ArrayList<>();
            }

            ParameterizedType ptype = (ParameterizedType) type;
            Type[] targs = ptype.getActualTypeArguments();

            for (Type aType : targs) {

                classes.add(extractClass(ownerClass, aType));
            }
        }
        return classes;
    }

    private Class<?> extractClass(Class<?> ownerClass, Type arg) {
        if (arg instanceof ParameterizedType) {
            return extractClass(ownerClass, ((ParameterizedType) arg).getRawType());
        } else if (arg instanceof GenericArrayType) {
            throw new UnsupportedOperationException("GenericArray types are not supported.");
        } else if (arg instanceof TypeVariable) {
            throw new UnsupportedOperationException("GenericArray types are not supported.");
        }
        return (arg instanceof Class ? (Class<?>) arg : Object.class);
    }
}
