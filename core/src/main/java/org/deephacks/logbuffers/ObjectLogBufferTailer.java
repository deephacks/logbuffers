package org.deephacks.logbuffers;


import javax.lang.model.type.TypeVariable;
import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ObjectLogBufferTailer<T> {
    private ObjectLogBuffer logBuffer;
    private Index readIndex;
    private Tail<T> tail;
    private Class<T> type;

    public ObjectLogBufferTailer(ObjectLogBuffer logBuffer, Tail<T> tail) throws IOException {
        this.logBuffer = logBuffer;
        this.readIndex = new Index(logBuffer.getBasePath() + "/" + tail.getName());
        this.tail = tail;
        this.type = (Class<T>) getParameterizedType(tail.getClass(), Tail.class).get(0);
    }

    public void forward() throws IOException {
        long currentWriteIndex = logBuffer.getIndex();
        long currentReadIndex = readIndex.getIndex();
        List<T> messages = logBuffer.select(type, currentReadIndex, currentWriteIndex);
        tail.process(messages);
        // only write the read index if tail was successful
        readIndex.writeIndex(currentWriteIndex);
    }

    public void forwardWithFixedDelay(int delay, TimeUnit unit) {
        this.logBuffer.getCachedExecutor().scheduleWithFixedDelay(new TailSchedule(this), 0, delay, unit);
    }

    private static final class TailSchedule implements Runnable {
        private ObjectLogBufferTailer tailer;

        public TailSchedule(ObjectLogBufferTailer tailer) {
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
