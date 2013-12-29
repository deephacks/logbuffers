package org.deephacks.logbuffers;

import javax.lang.model.type.TypeVariable;
import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The actual process that watch the log buffer for new logs.
 *
 * If scheduled forwarding is used, a single-threaded executor of the underlying log buffer
 * will be reused for every tail instance.
 *
 * This process consumes all logs of any type.
 */
class LogBufferTail<T> {
  private LogBuffer logBuffer;
  private Index readIndex;
  private Tail<T> tail;
  private ScheduledFuture<?> scheduledFuture;
  private Class<T> type;
  private String tailId;

  LogBufferTail(LogBuffer logBuffer, Tail<T> tail) throws IOException {
    this.logBuffer = logBuffer;
    this.tail = tail;
    this.type = (Class<T>) getParameterizedType(tail.getClass(), Tail.class).get(0);
  }

  String getTailId() {
    if (tailId == null) {
      tailId = logBuffer.getBasePath() + "/" + tail.getClass().getName();
    }
    return tailId;
  }

  /**
   * Push the index forward if logs are processed successfully by the tail.
   *
   * @throws IOException
   */
  void forward() throws IOException {
    if (readIndex == null) {
      readIndex = new Index(getTailId());
    }
    /**
     * Fix paging mechanism to handle when read and write indexes are too far apart!
     */
    long currentWriteIndex = logBuffer.getWriteIndex();
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
  synchronized void forwardWithFixedDelay(int delay, TimeUnit unit) {
    if (scheduledFuture != null) {
      return;
    }
    scheduledFuture = this.logBuffer.getCachedExecutor().scheduleWithFixedDelay(new TailSchedule(this), 0, delay, unit);
  }

  /**
   * Cancel the periodic tail task.
   *
   * @param mayInterruptIfRunning if the thread executing this
   * task should be interrupted; otherwise, in-progress tasks are allowed
   * to complete
   */
  synchronized void cancel(boolean mayInterruptIfRunning) {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(mayInterruptIfRunning);
    }
  }

  /**
   * @return the current read index.
   */
  synchronized long getReadIndex() throws IOException {
    return readIndex.getIndex();
  }

  /**
   * @throws IOException
   */
  public void close() throws IOException {
    readIndex.close();
  }

  private static final class TailSchedule implements Runnable {
    private LogBufferTail tailer;

    public TailSchedule(LogBufferTail tailer) {
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
      types = new Type[]{ownerClass.getGenericSuperclass()};
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
