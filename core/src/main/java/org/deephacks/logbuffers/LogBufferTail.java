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

import org.deephacks.logbuffers.TailForwardResult.ScheduleAgain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The actual process that watch the log buffer for new logs.
 * <p/>
 * If scheduled forwarding is used, a single-threaded executor of the underlying log buffer
 * will be reused for every tail instance.
 * <p/>
 * This process consumes all logs of any type.
 */
class LogBufferTail {
  private final Logger logger;
  protected LogBuffer logBuffer;
  protected Tail tail;
  protected final Index readIndex;
  private ScheduledFuture<?> scheduledFuture;
  private String tailId;

  LogBufferTail(LogBuffer logBuffer, TailSchedule schedule) throws IOException {
    this.logBuffer = logBuffer;
    logBuffer.initalizeDirs();
    this.tail = schedule.getTail();
    this.readIndex = Index.binaryIndex(getTailId());
    long[] lastSeen = this.readIndex.getLastSeen();
    if (lastSeen[1] == -1 || schedule.getStarTime().isPresent()) {
      setStartReadTime(schedule.getStarTime().orElse(0L));
    }
    this.logger = LoggerFactory.getLogger(LogBuffer.class.getName() + "." + tailId);

  }

  String getTailId() {
    if (tailId == null) {
      logBuffer.mkdirsBasePath();
      tailId = logBuffer.getBasePath() + "/" + tail.getClass().getName();
    }
    return tailId;
  }

  /**
   * Push the index forward if logs are processed successfully by the tail.
   *
   * @throws IOException
   */
  TailForwardResult forward() throws IOException {
    long[] seen = readIndex.getLastSeen();
    long seenTime = seen[0];
    long seenIndex = seen[1];
    Dirs.LogIterator it;

    if (seenIndex == -1) {
      long now = System.currentTimeMillis();
      logger.debug("forwardTime {} {}", seenTime, now);
      it = new Dirs.LogIterator(logBuffer.dirs, Query.closedTime(seenTime, now));
    } else {
      logger.debug("forwardIndex atLeast {}", seenIndex + 1);
      it = new Dirs.LogIterator(logBuffer.dirs, Query.atLeastIndex(seenIndex + 1));
    }
    try {
      tail.process(new Logs(Guavas.toStream(it, false)));
      Log lastProcessed = it.getLastProcessed();
      // only write the read index if tail was successful
      if (lastProcessed != null) {
        readIndex.writeLastSeen(lastProcessed.getTimestamp(), lastProcessed.getIndex());
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    return new TailForwardResult();
  }

  /**
   * Forwards the log processing periodically by notifying the tail each round.
   *
   * @param delay the delay between the termination of one execution and the commencement of the next.
   * @param unit  fromTime unit of the delay parameter.
   */
  synchronized void forwardWithFixedDelay(int delay, TimeUnit unit) {
    if (scheduledFuture != null) {
      return;
    }
    scheduledFuture = this.logBuffer.getCachedExecutor().scheduleWithFixedDelay(new TailScheduler(this, logBuffer.getCachedExecutor()), 0, delay, unit);
  }

  public void forwardNow() {
    this.logBuffer.getCachedExecutor().schedule(new TailScheduler(this, logBuffer.getCachedExecutor()), 0, TimeUnit.MILLISECONDS);
  }

  /**
   * Cancel the periodic tail task.
   *
   * @param mayInterruptIfRunning if the thread executing this
   *                              task should be interrupted; otherwise, in-progress tasks are allowed
   *                              to complete
   */
  synchronized void cancel(boolean mayInterruptIfRunning) {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(mayInterruptIfRunning);
    }
  }

  Long setStartReadTime(long time) throws IOException {
    readIndex.writeLastSeen(time, -1);
    return time;
  }

  private static final class TailScheduler implements Runnable {
    private LogBufferTail tail;
    private ScheduledExecutorService executor;

    public TailScheduler(LogBufferTail tail, ScheduledExecutorService executor) {
      this.tail = tail;
      this.executor = executor;
    }

    @Override
    public void run() {
      try {
        TailForwardResult forwardResult = tail.forward();
        Optional<ScheduleAgain> scheduleAgain = forwardResult.scheduleAgain();
        if (scheduleAgain.isPresent()) {
          executor.schedule(this, scheduleAgain.get().getDelay(), scheduleAgain.get().getTimeUnit());
        }
      } catch (AbortRuntimeException e) {
        e.printStackTrace();
        executor.shutdownNow();
      } catch (Throwable e) {
        // ignore for now
        e.printStackTrace();
      }
    }
  }
}
