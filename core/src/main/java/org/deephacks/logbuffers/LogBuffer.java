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

import net.openhft.chronicle.ChronicleConfig;
import org.deephacks.vals.Encodable;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.deephacks.logbuffers.Dirs.Dir;
import static org.deephacks.logbuffers.Guavas.checkNotNull;

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
  private AppenderHolder appenderHolder;

  /** log reader */
  Dirs dirs;

  /** path where log buffer files are stored */
  private File basePath;

  private ConcurrentHashMap<Class, LogBufferTail> tails = new ConcurrentHashMap<>();

  private RollingRanges ranges;

  private final Optional<Integer> readersMaxRollingFiles;

  private final ChronicleConfig config;

  protected LogBuffer(Builder builder) throws IOException {
    this.basePath = new File(builder.basePath.orElse(DEFAULT_BASE_PATH));
    this.logger = Logger.getLogger(LogBuffer.class.getName() + "." + checkNotNull(basePath + "/writer"));
    this.ranges = builder.ranges;
    this.readersMaxRollingFiles = builder.readersMaxRollingFiles;
    this.dirs = builder.dirs;
    this.config = builder.config;
  }

  // keep dirs lazy to avoid grabbing file descriptors where unnecessary
  Collection<Dir> initalizeDirs() {
    if (this.dirs == null) {
      synchronized (this) {
        if (dirs == null) {
          this.dirs = new Dirs(basePath, ranges, config);
          this.ranges = this.dirs.ranges;
        }
      }
    }
    return dirs.listDirs();
  }

  // keep tailers lazy to avoid grabbing file descriptors where unnecessary
  void initalizeAppenderHolder(long time) {
    if (this.appenderHolder == null) {
      synchronized (this) {
        if (appenderHolder == null) {
          this.appenderHolder = new AppenderHolder(basePath, Optional.ofNullable(ranges), time, config);
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
  public Log write(byte[] content) throws IOException {
    return internalWrite(new Log(System.currentTimeMillis(), content));
  }

  public Log write(Encodable encodable) throws IOException {
    Log log = new Log(System.currentTimeMillis(), (byte[]) null);
    initalizeAppenderHolder(log.getTimestamp());
    // single writer is required in order append to file since there is
    // only one file written to at a given fromTime. Also for generating unique
    // sequential indexes and sequential timestamps.
    synchronized (appenderHolder) {
      return log.write(encodable, appenderHolder);
    }
  }


  /**
   * Write a new raw log object.
   *
   * @param content raw content.
   * @throws IOException
   */
  public Log write(String content) throws IOException {
    return internalWrite(new Log(System.currentTimeMillis(), content));
  }

  private Log internalWrite(Log log) throws IOException {
    initalizeAppenderHolder(log.getTimestamp());
    // single writer is required in order append to file since there is
    // only one file written to at a given fromTime. Also for generating unique
    // sequential indexes and sequential timestamps.
    synchronized (appenderHolder) {
      return log.write(appenderHolder);
    }
  }

  /**
   * Get a specific log index.
   *
   * @param index to get
   * @return the log is present.
   * @throws IOException
   */
  public Optional<Log> getIndex(long index) throws IOException {
    initalizeDirs();
    Dir dir = dirs.getDir(index);
    if (dir == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(dir.getLog(index));
  }

  /**
   * Stream logs based on the given query. A query can be either time or indexed based.
   *
   * @param query
   * @return found logs.
   */
  public Logs find(Query query) {
    initalizeDirs();
    Dirs.LogIterator it = new Dirs.LogIterator(dirs, query);
    return new Logs(Guavas.toStream(it, false));
  }

  /**
   * Stream logs in parallel based on a set of directories.
   *
   * @return found logs.
   */
  public Logs parallel() {
    Collection<Dir> dirs = initalizeDirs();
    Stream<Log> result = null;
    for (Dir d : dirs) {
      Stream<Log> stream = Guavas.toStream(new Dirs.LogIterator(d), true);
      if (result == null) {
        result = stream;
      } else {
        result = Stream.concat(stream, result);
      }
    }
    if (result == null) {
      return new Logs(Stream.empty());
    }
    return new Logs(result);
  }

  /**
   * Forward the tail schedule manually.
   *
   * @param schedule schedule to forward
   * @return result of forward operation.
   * @throws IOException
   */

  public TailForwardResult forward(TailSchedule schedule) throws IOException {
    initalizeDirs();
    LogBufferTail tailBuffer = putIfAbsent(schedule);
    return tailBuffer.forward();
  }

  /**
   * Reset the read fromTime for the tail schedule.
   *
   * @param schedule  the schedule to modify
   * @param startTime start fromTime of the read
   * @return current read index
   * @throws IOException
   */
  public Long setReadTime(TailSchedule schedule, long startTime) throws IOException {
    LogBufferTail tailBuffer = putIfAbsent(schedule);
    return tailBuffer.setStartReadTime(startTime);
  }

  /**
   * @return directory where this log buffer is stored
   */
  public File getBasePath() {
    return basePath;
  }


  boolean mkdirsBasePath() {
    return basePath.mkdirs();
  }

  /**
   * Close this log buffer.
   *
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    if (appenderHolder != null) {
      synchronized (appenderHolder) {
        if (appenderHolder != null) {
          appenderHolder.close();
        }
      }
    }
    if (dirs != null) {
      synchronized (dirs) {
        for (Class<?> cls : tails.keySet()) {
          LogBufferTail logBufferTail = tails.remove(cls);
          logBufferTail.cancel(true);
        }
        if (cachedExecutor != null) {
          cachedExecutor.shutdown();
        }
        if (dirs != null) {
          dirs.close();
        }
      }
    }
  }

  /**
   * @return the current write writeIndex.
   * @throws IOException
   */
  public long getWriteIndex() throws IOException {
    long now = System.currentTimeMillis();
    initalizeAppenderHolder(now);
    synchronized (appenderHolder) {
      return appenderHolder.getAppenderIndex(now);
    }
  }

  /**
   * Cancel the periodic tail task.
   */
  public <T> void cancel(Class<? extends Tail> cls) throws IOException {
    cancel(cls, false);
  }

  /**
   * Cancel the periodic tail task.
   *
   * @param mayInterruptIfRunning if the thread executing this
   *                              task should be interrupted; otherwise, in-progress tasks are allowed
   *                              to complete
   */
  public <T> void cancel(Class<? extends Tail> cls, boolean mayInterruptIfRunning) throws IOException {
    LogBufferTail logBufferTail = tails.remove(cls);
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
    LogBufferTail logBufferTail = putIfAbsent(schedule);
    logBufferTail.forwardWithFixedDelay(schedule.getDelay(), schedule.getUnit());
  }

  private LogBufferTail putIfAbsent(TailSchedule schedule) throws IOException {
    Tail tail = schedule.getTail();
    LogBufferTail logBufferTail = tails.get(tail.getClass());
    if (logBufferTail == null || !schedule.isInitalized()) {
      logBufferTail = new LogBufferTail(this, schedule);
      schedule.markInitalized();
      tails.putIfAbsent(tail.getClass(), logBufferTail);
    }
    return logBufferTail;
  }

  public static class Builder {
    private ChronicleConfig config = ChronicleConfig.LARGE.clone();
    private Optional<String> basePath = Optional.empty();
    private Optional<Integer> readersMaxRollingFiles = Optional.empty();
    private Dirs dirs;
    private RollingRanges ranges;
    private Builder() {
      config.indexFileExcerpts(Integer.MAX_VALUE);
    }

    public Builder basePath(String basePath) {
      this.basePath = Optional.ofNullable(basePath);
      return this;
    }

    public Builder synchronousMode(boolean synchronousMode) {
      config.synchronousMode(synchronousMode);
      return this;
    }

    public Builder readersMaxRollingFiles(int readersMaxRollingFiles) {
      this.readersMaxRollingFiles = Optional.ofNullable(readersMaxRollingFiles);
      return this;
    }

    public Builder interval(TimeUnit unit) {
      switch(unit) {
        case NANOSECONDS:
        case MICROSECONDS:
        case MILLISECONDS:
          throw new IllegalArgumentException("not supported" + unit.name());
        case SECONDS:
          this.ranges = RollingRanges.secondly();
          break;
        case MINUTES:
          this.ranges = RollingRanges.minutely();
          break;
        case HOURS:
          this.ranges = RollingRanges.hourly();
          break;
        case DAYS:
          this.ranges = RollingRanges.daily();
          break;
      }
      return this;
    }

    public Builder ranges(RollingRanges ranges) {
      this.ranges = ranges;
      return this;
    }

    /**
     * Roll files every second.
     */
    public Builder secondly() {
      this.ranges = RollingRanges.secondly();
      return this;
    }

    /**
     * Roll files every minute.
     */
    public Builder minutely() {
      this.ranges = RollingRanges.minutely();
      return this;
    }

    /**
     * Roll files every hour.
     */
    public Builder hourly() {
      this.ranges = RollingRanges.hourly();
      return this;
    }

    /**
     * Roll files every day.
     */
    public Builder daily() {
      this.ranges = RollingRanges.hourly();
      return this;
    }

    // testing only
    Builder dirs(Dirs dirs) {
      this.dirs = dirs;
      this.ranges = dirs.ranges;
      return this;
    }

    public LogBuffer build() throws IOException {
      return new LogBuffer(this);
    }
  }
}
