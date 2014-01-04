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
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import org.deephacks.logbuffers.TailSchedule.TailScheduleChunk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

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

  /** this should be rolling, but there are bugs. Change later to VanillaChronicle in later versions */
  private IndexedChronicle rollingChronicle;

  /** log writer */
  private final ExcerptAppender excerptAppender;

  /** log reader */
  private final ExcerptTailer excerptTailer;

  /** path where log buffer files are stored */
  private String basePath;

  private ConcurrentHashMap<Class<?>, LogBufferTail<?>> tails = new ConcurrentHashMap<>();

  private LogSerializers serializers;

  protected LogBuffer(Builder builder) throws IOException {
    this.basePath = builder.basePath.or(DEFAULT_BASE_PATH);
    this.logger = Logger.getLogger(LogBuffer.class.getName() + "." + checkNotNull(basePath + "/writer"));
    this.rollingChronicle = new IndexedChronicle(basePath + "/data", builder.config);
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

  private LogRaw internalWrite(byte[] content, long type) throws IOException {
    // single writer is required in order append to file since there is
    // only one file written to at a given time. Also for generating unique
    // sequential indexes and sequential timestamps.
    synchronized (excerptAppender) {
      LogRaw log = new LogRaw(type, content);
      long index = log.write(excerptAppender);
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
    long writeIdx = getWriteIndex();
    return select(fromIndex, writeIdx);
  }

  Optional<LogRaw> get(long index) throws IOException {
    synchronized (excerptTailer) {
      return LogRaw.read(excerptTailer, index);
    }
  }

  /**
   * Get the next forward index of specified type.
   */
  public <T> Optional<LogRaw> getNext(Class<T> cls, long index) throws IOException {
    synchronized (excerptTailer) {
      long writeIndex = getWriteIndex();
      while (index < writeIndex) {
        Optional<Long> optional = peekType(index++);
        if (!optional.isPresent()) {
          continue;
        }
        long type = optional.get();
        if (type != LogRaw.DEFAULT_TYPE) {
          LogSerializer serializer = serializers.getSerializer(type);
          Class<?> found = serializer.getMapping().get(type);
          if (cls.isAssignableFrom(found)) {
            return get(index - 1);
          }
        } else {
          return get(index - 1);
        }
      }
    }
    return Optional.absent();
  }

  /**
   * Only reads the timestamp in order to avoid serialization overhead.
   */
  Optional<Long> peekTimestamp(long index) throws IOException {
    synchronized (excerptTailer) {
      return LogRaw.peekTimestamp(excerptTailer, index);
    }
  }

  Optional<Long> peekType(long index) throws IOException {
    synchronized (excerptTailer) {
      return LogRaw.peekType(excerptTailer, index);
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
   *
   * @param starTime time to search for.
   * @return closest matching index
   * @throws IOException
   */
  public Long findStartTimeIndex(long starTime) throws IOException {
    long writeIndex = getWriteIndex();
    synchronized (excerptTailer) {
      long index = binarySearchStartTime(writeIndex, starTime);
      while (index > 0 && index < (writeIndex - 1)) {
        if (starTime <= get(index - 1).get().getTimestamp()) {
          // index too far to the right
          index--;
        } else if (starTime > get(index).get().getTimestamp()) {
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
   * Search the closest index to start time using binary search.
   */
  private Long binarySearchStartTime(long writeIndex, long startTime) throws IOException {
    long low = 0;
    long high = writeIndex;
    synchronized (excerptTailer) {
      while (low < high) {
        long mid = (low + high) >>> 1;
        long timestamp = get(mid).get().getTimestamp();
        if (timestamp < startTime)
          low = mid + 1;
        else if (timestamp > startTime)
          high = mid - 1;
        else {
          return mid;
        }
      }
      return low >= writeIndex ? writeIndex - 1 : low;
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
    List<LogRaw> messages = new ArrayList<>();
    synchronized (excerptTailer) {
      long read = fromIndex;
      while (read < toIndex) {
        Optional<LogRaw> optional = get(read++);
        if (!optional.isPresent()) {
          break;
        }
        messages.add(optional.get());
      }
      return messages;
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
    return selectBackward(getWriteIndex() - 1, fromTimeMs, toTimeMs);
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
    return selectForward(getWriteIndex() - 1, fromTimeMs, toTimeMs);
  }

  /**
   * Select a list of logs based on the given period of time with respect
   * to the timestamp of each log, start scanning at from index, going
   * backwards in time.
   *
   * @param fromTimeMs from (inclusive)
   * @param toTimeMs   to (inclusive)
   * @param fromIndex  from what index to start scanning
   * @return list of matching logs
   * @throws IOException
   */
  public List<LogRaw> selectBackward(long fromIndex, long fromTimeMs, long toTimeMs) throws IOException {
    Preconditions.checkArgument(fromTimeMs <= toTimeMs, "from must be less than to");
    LinkedList<LogRaw> messages = new LinkedList<>();
    synchronized (excerptTailer) {
      for (long i = fromIndex; i > -1; i--) {
        Optional<Long> optional = peekTimestamp(i);
        if (!optional.isPresent()) {
          continue;
        }
        long timestamp = optional.get();
        if (timestamp >= fromTimeMs && timestamp <= toTimeMs) {
          messages.addFirst(get(i).get());
        }
        if (timestamp < fromTimeMs) {
          // moved past fromTime and since all timestamps are sequential
          // there is no more data to find at this point.
          break;
        }
      }
      return messages;
    }
  }

  /**
   * Select a list of logs based on the given period of time with respect
   * to the timestamp of each log, start scanning at from index, going
   * forwards in time.
   *
   * @param fromTimeMs from (inclusive)
   * @param toTimeMs   to (inclusive)
   * @param fromIndex  from what index to start scanning
   * @return list of matching logs
   * @throws IOException
   */
  public List<LogRaw> selectForward(long fromIndex, long fromTimeMs, long toTimeMs) throws IOException {
    Preconditions.checkArgument(fromTimeMs <= toTimeMs, "from must be less than to");
    LinkedList<LogRaw> messages = new LinkedList<>();
    long writeIndex = getWriteIndex();
    synchronized (excerptTailer) {
      for (long i = fromIndex; i < writeIndex; i++) {
        Optional<Long> optional = peekTimestamp(i);
        if (!optional.isPresent()) {
          continue;
        }
        long timestamp = optional.get();
        if (timestamp >= fromTimeMs && timestamp <= toTimeMs) {
          messages.addLast(get(i).get());
        }
        if (timestamp > toTimeMs) {
          // moved past fromTime and since all timestamps are sequential
          // there is no more data to find at this point.
          break;
        }
      }
      return messages;
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
    return select(type, fromIndex, getWriteIndex());
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
   *
   * {@link #selectBackward(long, long, long)}
   */
  public <T> Logs<T> selectBackward(Class<T> type, long fromTimeMs, long toTimeMs) throws IOException {
    final List<LogRaw> logs = selectBackward(fromTimeMs, toTimeMs);
    return convert(type, logs);
  }

  /**
   * Selects a specific type of logs only, all other types are filtered out, based on the
   * given period of time with respect to the timestamp of each log.
   * <p/>
   *
   * {@link #selectForward(Class, long, long, long)}
   */
  public <T> Logs<T> selectForward(Class<T> type, long fromIndex, long fromTimeMs, long toTimeMs) throws IOException {
    final List<LogRaw> logs = selectForward(fromIndex, fromTimeMs, toTimeMs);
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

  public <T> TailForwardResult forward(TailSchedule schedule) throws IOException {
    LogBufferTail<T> tailBuffer = putIfAbsent(schedule);
    return tailBuffer.forward();
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
    synchronized (excerptAppender) {
      excerptAppender.close();
      for (Class<?> cls : tails.keySet()) {
        LogBufferTail<?> logBufferTail = tails.remove(cls);
        logBufferTail.cancel(true);
        logBufferTail.close();
      }
      if (cachedExecutor != null) {
        cachedExecutor.shutdown();
      }
      excerptTailer.close();
      rollingChronicle.close();
    }
  }

  /**
   * @return the current write writeIndex.
   * @throws IOException
   */
  public long getWriteIndex() throws IOException {
    synchronized (excerptAppender) {
      return excerptAppender.index();
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

  public static class Builder {
    private ChronicleConfig config = ChronicleConfig.LARGE.clone();
    private Optional<String> basePath = Optional.absent();
    private LogSerializers serializers = new LogSerializers();

    public Builder() {
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

    public LogBuffer build() throws IOException {
      return new LogBuffer(this);
    }
  }
}
