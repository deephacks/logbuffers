package org.deephacks.logbuffers;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Specialized tail that provide logs iteratively in chunks according to a certain period of time.
 */
class LogBufferTailChunk<T> extends LogBufferTail<T> {
  private static SimpleDateFormat FORMAT = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss:SSS");
  private long chunkMs;
  private File readTime;

  LogBufferTailChunk(LogBuffer logBuffer, Tail<T> tail, long chunkMs) throws IOException {
    super(logBuffer, tail);
    this.chunkMs = chunkMs;
    this.readTime = new File(getTailId() + ".lastRead_date_timestamp_index");
  }

  @Override
  ForwardResult forward() throws IOException {
    // fetch latest written log to determine how far ahead the
    // writer index is so tail can speed up if backlog is too big.
    Optional<Log> latestWrite = logBuffer.getLatestWrite();
    if (!latestWrite.isPresent()) {
      return new ForwardResult();
    }

    // the index that was written by previous tail acknowledgement
    long currentReadIndex = getReadIndex();
    Optional<Log> currentLog = logBuffer.get(currentReadIndex);
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
    Log lastRead = logs.getLastLog();
    long lastReadIndex = lastRead.getIndex();
    // prepare result
    ForwardResult result = new ForwardResult();
    if (lastRead.getTimestamp() < latestWrite.get().getTimestamp()) {
      // alter the result to indicate that there are already more logs
      // to process after this round have been executed. Hence we can
      // act quickly and process these as fast as possible, if needed.
      result = new ForwardResult(false);
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
      result = new ForwardResult();
      System.err.println(e.getMessage());
    }
    return result;
  }

  private void writeHumanReadableTime(Log lastRead) {
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
