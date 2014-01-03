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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import org.deephacks.logbuffers.TailSchedule.TailScheduleChunk;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Specialized tail that provide logs iteratively in chunks according to a certain period of time.
 */
class LogBufferTailChunk<T> extends LogBufferTail<T> {
  private static SimpleDateFormat FORMAT = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss:SSS");
  private long chunkMs;
  private File readTime;
  private long rescheduleDelay = 0;
  private TimeUnit rescheduleTimeUnit = TimeUnit.MILLISECONDS;

  LogBufferTailChunk(LogBuffer logBuffer, TailScheduleChunk schedule) throws IOException {
    super(logBuffer, (Tail<T>) schedule.getTail());
    this.chunkMs = schedule.getChunkMs();
    this.rescheduleDelay = schedule.getBackLogScheduleDelay();
    this.rescheduleTimeUnit = schedule.getBackLogScheduleUnit();
    this.readTime = new File(getTailId() + ".lastRead_date_timestamp_index");
  }

  @Override
  ForwardResult forward() throws IOException {
    // fetch latest written log to determine how far ahead the
    // writer index is so tail can speed up if backlog is too big.
    Optional<RawLog> latestWrite = logBuffer.getLatestWrite();
    if (!latestWrite.isPresent()) {
      return new ForwardResult();
    }
    // the index that was written by previous tail acknowledgement
    long currentReadIndex = getReadIndex();
    Optional<RawLog> currentLog = logBuffer.getNext(type, currentReadIndex);
    if (!currentLog.isPresent()) {
      return new ForwardResult();
    }
    // pick the next log by trying to select fixed chunk period
    long fixedFrom = fix(currentLog.get().getTimestamp());
    long fixedTo = fixedFrom + chunkMs - 1;
    // do not process ahead of time, meaning tail will not try process
    // logs until the chunkMs have passed since the present.
    if (fixedTo > System.currentTimeMillis()) {
      return new ForwardResult();
    }

    // start directly at index where processing ended previous round go forward in time
    Logs<T> logs = logBuffer.selectForward(type, currentLog.get().getIndex(), fixedFrom, fixedTo);

    // don't call tail if there are no logs
    if (logs.isEmpty()) {
      return new ForwardResult();
    }
    // prepare the next read index BEFORE we hand over logs to tail
    RawLog lastRead = logs.getLastLog();
    long lastReadIndex = lastRead.getIndex();
    // prepare result
    ForwardResult result = new ForwardResult();
    if (lastRead.getTimestamp() < latestWrite.get().getTimestamp()) {
      // alter the result to indicate that there are already more logs
      // to process after this round have been executed. Hence we can
      // act quickly and process these as fast as possible, if needed.
      result = ForwardResult.scheduleAgain(rescheduleDelay, rescheduleTimeUnit);
    }
    try {
      // ready to process logs. ignore any exceptions since the LogBuffer
      // will take care of them and retry automatically for us next round.
      // haven't persistent anything to disk yet so tail is fine if it happens
      tail.process(logs);
      // only write/persist last read index if tail was successful
      writeReadIndex(lastReadIndex + 1);
      writeHumanReadableTime(lastRead);
    } catch (Exception e) {
      // cancel any immediate scheduling if log processing failed
      result = new ForwardResult();
      System.err.println(e.getMessage());
    }
    return result;
  }

  private void writeHumanReadableTime(RawLog lastRead) {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(FORMAT.format(new Date(lastRead.getTimestamp()))).append(' ');
      sb.append(lastRead.getTimestamp()).append(' ');
      sb.append(lastRead.getIndex()).append('\n');
      Files.write(sb.toString().getBytes(Charsets.UTF_8), readTime);
    } catch (IOException e) {
      System.err.println("Could not write to " + readTime.getAbsolutePath());
    }
  }

  /**
   * Calculate a fixed period of time aligned with the chunk length.
   */
  private long fix(long timeMs) {
    return timeMs - (timeMs % chunkMs);
  }
}
