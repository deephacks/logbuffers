/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.logbuffers;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import net.openhft.chronicle.ChronicleConfig;
import org.deephacks.logbuffers.TailSchedule.TailScheduleChunk;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.deephacks.logbuffers.TailerHolder.Tailer;

/**
 * A log buffer persist data items sequentially on local disk.
 * <p/>
 * Every written log is assigned a unique writeIndex identifier that works like an sequential offset.
 * Indexes are used by readers to select one or more logs.
 * <p/>
 * A logical log buffer is divided into a set of files in a configurable directory.
 * New files are created when the capacity of the current file is reached. Files
 * are not deleted at the moment.
 * <p/>
 * The physical separation of a log buffer is an implementation detail that the user
 * does not need to care about.
 */
public class LogBuffer {

  private final Logger logger;

  /** system tmp dir */
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

  /** default path used by log files if not specified */
  private static final String DEFAULT_BASE_PATH = TMP_DIR + "/logbuffer";

  /** optional executor used only by scheduled tailing */
  private ScheduledExecutorService cachedExecutor;

  /** log writer */
  private final AppenderHolder appenderHolder;

  /** log reader */
  private TailerHolder tailerHolder;

  /** path where log buffer files are stored */
  private String basePath;

  private ConcurrentHashMap<Class<?>, LogBufferTail<?>> tails = new ConcurrentHashMap<>();

  private LogSerializers serializers;

  private final DateRanges ranges;

  protected LogBuffer(Builder builder) throws IOException {
    Preconditions.checkNotNull(builder.ranges, "choose a range");
    this.basePath = builder.basePath.or(DEFAULT_BASE_PATH);
    this.logger = Logger.getLogger(LogBuffer.class.getName() + "." + checkNotNull(basePath + "/writer"));
    this.appenderHolder = new AppenderHolder(basePath + "/data", builder.ranges);
    this.serializers = builder.serializers;
    this.ranges = builder.ranges;
  }

  // keep tailers lazy to avoid grabbing file descriptors where unnecessary
  private void initalizeTailerHolder() {
    if (this.tailerHolder == null) {
      synchronized (this) {
        if (tailerHolder == null) {
          this.tailerHolder = new TailerHolder(basePath + "/data", ranges);
        }
      }
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  synchronized ScheduledExecutorService getCachedExecutor() {
    if (cachedExecutor == null) {
      cachedExecutor = Executors.newSingleThreadScheduledExecutor();
    }
    return cachedExecutor;
  }

  /**
   * Write a new raw log object.
   *
   * @param content raw content.
   * @throws IOException
   */
  public LogRaw write(byte[] content) throws IOException {
    return internalWrite(content, LogRaw.DEFAULT_TYPE);
  }

  /**
   * Write an object into the log buffer.
   *
   * @param object to be written
   * @return the log that was created.
   * @throws IOException
   */
  public LogRaw write(Object object) throws IOException {
    Class<?> cls = object.getClass();
    LogSerializer serializer = serializers.getSerializer(cls);
    byte[] content = serializer.serialize(object);
    return internalWrite(content, serializers.getType(cls));
  }

  void tryWrite(LogRaw log) {

  }

  private LogRaw internalWrite(byte[] content, long type) throws IOException {
    // single writer is required in order append to file since there is
    // only one file written to at a given time. Also for generating unique
    // sequential indexes and sequential timestamps.
    synchronized (appenderHolder) {
      LogRaw log = new LogRaw(type, content);
      long index = log.write(appenderHolder);
      return new LogRaw(log, index);
    }
  }

  /**
   * Select a list of log objects from a specific writeIndex up until the
   * most recent log written.
   *
   * @param fromIndex writeIndex to read from.
   * @return A list of raw object logs.
   * @throws IOException
   */
  public List<LogRaw> select(long fromIndex) throws IOException {
    initalizeTailerHolder();
    long lastIndex = tailerHolder.getLatestStopIndex();
    return select(fromIndex, lastIndex);
  }

  Optional<LogRaw> get(long index) throws IOException {
    initalizeTailerHolder();
    synchronized (tailerHolder) {
      return LogRaw.read(tailerHolder, index);
    }
  }

  /**
   * Get the next forward index of specified type.
   */
  public <T> Optional<LogRaw> getNext(Class<T> cls, long index) throws IOException {
    initalizeTailerHolder();
    synchronized (tailerHolder) {
      Optional<Long> optional = peekType(index);
      if (!optional.isPresent()) {
        return Optional.absent();
      }
      long type = optional.get();
      if (type != LogRaw.DEFAULT_TYPE) {
        LogSerializer serializer = serializers.getSerializer(type);
        Class<?> found = serializer.getMapping().get(type);
        if (cls.isAssignableFrom(found)) {
          return get(index);
        } else {
          return getNext(cls, ++index);
        }
      } else {
        return get(index);
      }
    }
  }

  /**
   * Only reads the timestamp in order to avoid serialization overhead.
   */
  Optional<Long> peekTimestamp(long index) throws IOException {
    initalizeTailerHolder();
    synchronized (tailerHolder) {
      List<Tailer> tailers = tailerHolder.getTailersBetweenIndex(index, index);
      return LogRaw.peekTimestamp(tailers.get(0), index);
    }
  }

  Optional<Long> peekType(long index) throws IOException {
    initalizeTailerHolder();
    synchronized (tailerHolder) {
      return LogRaw.peekType(tailerHolder, index);
    }
  }

  Optional<LogRaw> getLatestWrite() throws IOException {
    long index = getWriteIndex();
    if (index == 0) {
      return get(0);
    }
    return get(index - 1);
  }

  /**
   * Return the index closest to provided time.
   * <p/>
   * This is a very fast scan operation. Should find the index in less than
   * a millisecond in a buffer with over 30 million logs.
   *
   * @param startTime time to search for.
   * @return closest matching index
   * @throws IOException
   */
  public Long findStartTimeIndex(long startTime) throws IOException {
    initalizeTailerHolder();
    long writeIndex = getWriteIndex();
    synchronized (tailerHolder) {
      long index = tailerHolder.binarySearchAfterTime(startTime).get().getIndex();
      while (index > 0 && index < (writeIndex - 1)) {
        // pick the lowest index for logs that have exact same timestamp
        if (startTime <= get(index - 1).get().getTimestamp()) {
          // index too far to the right
          index--;
        } else if (startTime > get(index).get().getTimestamp()) {
          // index too far to the left
          index++;
        } else {
          return index;
        }
      }
      return index;
    }
  }

  /**
   * Select a list of log objects from a specific writeIndex up until a
   * provided writeIndex.
   *
   * @param fromIndex writeIndex to read from.
   * @param toIndex   writeIndex to read up until.
   * @return A list of raw object logs.
   * @throws IOException
   */
  public List<LogRaw> select(long fromIndex, long toIndex) throws IOException {
    Preconditions.checkArgument(fromIndex <= toIndex, "from must be less than to");
    initalizeTailerHolder();
    synchronized (tailerHolder) {
      ListIterator<Tailer> tailers = tailerHolder.getTailersBetweenIndex(fromIndex, toIndex).listIterator();
      List<LogRaw> result = new ArrayList<>();
      if (!tailers.hasNext()) {
        return new ArrayList<>();
      }
      Tailer tailer = tailers.next();
      // pick first known index
      long currentIndex = fromIndex < tailer.startIndex ? tailer.startIndex : fromIndex;
      while (true) {
        long lastWrittenIndex = tailer.getLastWrittenIndex();
        while (currentIndex <= lastWrittenIndex) {
          Optional<LogRaw> optional = LogRaw.read(tailer, currentIndex++);
          if (!optional.isPresent()) {
            break;
          }
          result.add(optional.get());
        }
        if (tailers.hasNext()) {
          tailer = tailers.next();
          currentIndex = tailer.startIndex;
        } else {
          return result;
        }
      }
    }
  }

  /**
   * Select a list of logs based on the given period of time with respect
   * to the timestamp of each log.
   *
   * @param fromTimeMs from (inclusive)
   * @param toTimeMs   to (inclusive)
   * @return list of matching logs
   * @throws IOException
   */
  public List<LogRaw> selectBackward(long fromTimeMs, long toTimeMs) throws IOException {
    // improve performance with a real implementation
    List<LogRaw> logs = selectForward(toTimeMs, fromTimeMs);
    Collections.reverse(logs);
    return logs;
  }

  /**
   * Select a list of logs based on the given period of time with respect
   * to the timestamp of each log.
   *
   * @param fromTimeMs from (inclusive)
   * @param toTimeMs   to (inclusive)
   * @return list of matching logs
   * @throws IOException
   */
  public List<LogRaw> selectForward(long fromTimeMs, long toTimeMs) throws IOException {
    Preconditions.checkArgument(fromTimeMs <= toTimeMs, "from must be less than to");
    initalizeTailerHolder();
    LinkedList<LogRaw> messages = new LinkedList<>();
    ListIterator<Tailer> tailers = tailerHolder.getTailersBetweenTime(fromTimeMs, toTimeMs).listIterator();
    if (!tailers.hasNext()) {
      return new ArrayList<>();
    }
    Tailer t = tailers.next();
    Optional<LogRaw> fromLog = t.binarySearchAfterTime(fromTimeMs);
    if (!fromLog.isPresent()) {
      return new ArrayList<>();
    }
    long startIndex = fromLog.get().getIndex();
    synchronized (tailerHolder) {
      while (true) {
        for (long i = startIndex; i < Long.MAX_VALUE; i++) {
          Optional<Long> optional = LogRaw.peekTimestamp(t, i);
          if (!optional.isPresent()) {
            break;
          }
          long timestamp = optional.get();
          if (timestamp >= fromTimeMs && timestamp <= toTimeMs) {
            messages.addLast(LogRaw.read(t, i).get());
          }
          if (timestamp > toTimeMs) {
            // moved past fromTime and since all timestamps are sequential
            // there is no more data to find at this point.
            break;
          }
        }
        if (tailers.hasNext()) {
          t = tailers.next();
          startIndex = t.startIndex;
        } else {
          return messages;
        }
      }
    }
  }

  /**
   * Selects logs only of a specific type, all other types are filtered out.
   *
   * @param type      the type of logs to be selected.
   * @param fromIndex writeIndex to read from.
   * @return A list of object logs.
   * @throws IOException
   */
  public <T> Logs<T> select(Class<T> type, long fromIndex) throws IOException {
    initalizeTailerHolder();
    return select(type, fromIndex, tailerHolder.getLatestStopIndex());
  }

  /**
   * Selects a specific type of logs only, all other types are filtered out.
   *
   * @param type      the type of logs to be selected.
   * @param fromIndex writeIndex to read from.
   * @param toIndex   writeIndex to read up until.
   * @return A list of object logs.
   * @throws IOException
   */
  public <T> Logs<T> select(Class<T> type, long fromIndex, long toIndex) throws IOException {
    List<LogRaw> logs = select(fromIndex, toIndex);
    return convert(type, logs);
  }

  /**
   * Selects a specific type of logs only, all other types are filtered out, based on the
   * given period of time with respect to the timestamp of each log.
   * <p/>
   */
  public <T> Logs<T> selectBackward(Class<T> type, long fromTimeMs, long toTimeMs) throws IOException {
    final List<LogRaw> logs = selectBackward(fromTimeMs, toTimeMs);
    return convert(type, logs);
  }

  /**
   * Selects a specific type of logs only, all other types are filtered out, based on the
   * given period of time with respect to the timestamp of each log.
   * <p/>
   */
  public <T> Logs<T> selectForward(Class<T> type, long fromTimeMs, long toTimeMs) throws IOException {
    final List<LogRaw> logs = selectForward(fromTimeMs, toTimeMs);
    return convert(type, logs);
  }

  private <T> Logs<T> convert(Class<T> type, List<LogRaw> logs) {
    Logs<T> result = new Logs<>();
    for (LogRaw log : logs) {
      if (log.getType() != LogRaw.DEFAULT_TYPE) {
        LogSerializer serializer = serializers.getSerializer(log.getType());
        if (serializer == null) {
          throw new IllegalStateException("No serializer found for type " + log.getType());
        }
        Class<?> cls = serializer.getMapping().get(log.getType());
        if (type.isAssignableFrom(cls)) {
          T object = (T) serializer.deserialize(log.getContent(), log.getType());
          result.put(object, log);
        }
      } else {
        if (type.isAssignableFrom(LogRaw.class)) {
          result.put((T) log, log);
        }
      }
    }
    return result;
  }

  /**
   * Forward the tail schedule manually.
   *
   * @param schedule schedule to forward
   * @return result of forward operation.
   * @throws IOException
   */

  public <T> TailForwardResult forward(TailSchedule schedule) throws IOException {
    LogBufferTail<T> tailBuffer = putIfAbsent(schedule);
    return tailBuffer.forward();
  }

  /**
   * Reset the read time for the tail schedule.
   *
   * @param schedule  the schedule to modify
   * @param startTime start time of the read
   * @return current read index
   * @throws IOException
   */
  public Long setReadTime(TailSchedule schedule, long startTime) throws IOException {
    LogBufferTail<?> tailBuffer = putIfAbsent(schedule);
    return tailBuffer.setStartReadTime(startTime);
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
    synchronized (appenderHolder) {
      appenderHolder.close();
      for (Class<?> cls : tails.keySet()) {
        LogBufferTail<?> logBufferTail = tails.remove(cls);
        logBufferTail.cancel(true);
        logBufferTail.close();
      }
      if (cachedExecutor != null) {
        cachedExecutor.shutdown();
      }
      if (tailerHolder != null) {
        tailerHolder.close();
      }
    }
  }

  /**
   * @return the current write writeIndex.
   * @throws IOException
   */
  public long getWriteIndex() throws IOException {
    synchronized (appenderHolder) {
      return appenderHolder.getAppenderIndex(System.currentTimeMillis());
    }
  }

  /**
   * Cancel the periodic tail task.
   */
  public <T> void cancel(Class<? extends Tail<T>> cls) throws IOException {
    cancel(cls, false);
  }

  /**
   * Cancel the periodic tail task.
   *
   * @param mayInterruptIfRunning if the thread executing this
   *                              task should be interrupted; otherwise, in-progress tasks are allowed
   *                              to complete
   */
  public <T> void cancel(Class<? extends Tail<T>> cls, boolean mayInterruptIfRunning) throws IOException {
    LogBufferTail<?> logBufferTail = tails.remove(cls);
    if (logBufferTail != null) {
      logBufferTail.cancel(mayInterruptIfRunning);
    }
  }

  /**
   * Forwards the log processing periodically by notifying the tail each round. All logs that are unprocessed
   * will be given each round. Logs are not duplicated. If a failure occur, all unprocessed logs are
   * retried next round.
   *
   * @param schedule the schedule description for the tail
   */
  public void forwardWithFixedDelay(TailSchedule schedule) throws IOException {
    LogBufferTail<?> logBufferTail = putIfAbsent(schedule);
    logBufferTail.forwardWithFixedDelay(schedule.getDelay(), schedule.getUnit());
  }

  /**
   * Forwards the log processing periodically by notifying the tail each round. Logs are given
   * iteratively in chunks according to a certain period of time until all unprocessed logs are
   * finished. Logs are not duplicated. If a failure occur, the same chunk is retried next round.
   *
   * @param schedule the schedule description for the tail
   * @throws IOException
   */
  public void forwardWithFixedDelay(TailScheduleChunk schedule) throws IOException {
    LogBufferTail<?> logBufferTail = putIfAbsent(schedule);
    logBufferTail.forwardWithFixedDelay(schedule.getDelay(), schedule.getUnit());
  }

  private <T> LogBufferTail<T> putIfAbsent(TailSchedule schedule) throws IOException {
    Tail<?> tail = schedule.getTail();
    LogBufferTail<?> logBufferTail = tails.putIfAbsent(tail.getClass(), new LogBufferTail<>(this, schedule));
    if (logBufferTail == null) {
      logBufferTail = tails.get(tail.getClass());
      logBufferTail.writeReadIndex(getFirstIndex());
    }
    return (LogBufferTail<T>) logBufferTail;
  }

  private <T> LogBufferTail<T> putIfAbsent(TailScheduleChunk schedule) throws IOException {
    Tail<?> tail = schedule.getTail();
    LogBufferTail<?> logBufferTail = tails.putIfAbsent(tail.getClass(), new LogBufferTailChunk<>(this, schedule));
    if (logBufferTail == null) {
      logBufferTail = tails.get(tail.getClass());
    }
    return (LogBufferTail<T>) logBufferTail;
  }

  /**
   * Returns the index for a specific tail type.
   *
   * @return index for a specific tail type.
   * @throws IOException
   */
  public <T> long getReadIndex(Class<? extends Tail<T>> cls) throws IOException {
    LogBufferTail<?> logBufferTail = checkNotNull(tails.get(cls), "Tail type not registered " + cls);
    return logBufferTail.getReadIndex();
  }

  public long getFirstIndex() {
    initalizeTailerHolder();
    return tailerHolder.getFirstIndex();
  }

  public static class Builder {
    private ChronicleConfig config = ChronicleConfig.LARGE.clone();
    private Optional<String> basePath = Optional.absent();
    private LogSerializers serializers = new LogSerializers();
    private DateRanges ranges;

    private Builder() {
      config.indexFileExcerpts(Short.MAX_VALUE);
    }

    public Builder basePath(String basePath) {
      this.basePath = Optional.fromNullable(basePath);
      return this;
    }

    public Builder addSerializer(LogSerializer serializer) {
      serializers.addSerializer(serializer);
      return this;
    }

    public Builder synchronousMode(boolean synchronousMode) {
      config.synchronousMode(synchronousMode);
      return this;
    }

    public Builder logsPerFile(int logsPerFile) {
      config.indexFileExcerpts(logsPerFile);
      return this;
    }

    /**
     * Roll files every second.
     */
    public Builder secondly() {
      this.ranges = DateRanges.secondly();
      return this;
    }

    /**
     * Roll files every minute.
     */
    public Builder minutely() {
      this.ranges = DateRanges.minutely();
      return this;
    }

    /**
     * Roll files every hour.
     */
    public Builder hourly() {
      this.ranges = DateRanges.hourly();
      return this;
    }

    /**
     * Roll files every day.
     */
    public Builder daily() {
      this.ranges = DateRanges.hourly();
      return this;
    }


    public LogBuffer build() throws IOException {
      return new LogBuffer(this);
    }
  }
}
