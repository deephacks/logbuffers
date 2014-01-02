package org.deephacks.logbuffers;

import com.google.common.base.Optional;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Specialized tail that provide logs iteratively in chunks according to a certain period of time.
 */
class LogBufferTailChunk<T> extends LogBufferTail<T> {
  private long chunkMs;
  public static SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");
  private final Logger logger;
  private Optional<Log> moreWork;

  LogBufferTailChunk(LogBuffer logBuffer, Tail<T> tail, long chunkMs) throws IOException {
    super(logBuffer, tail);
    this.chunkMs = chunkMs;
    this.logger = Logger.getLogger(getClass().getName());
    this.moreWork = Optional.absent();
  }

  @Override
  ForwardResult forward() throws IOException {
    // fetch latest written log to determine how far ahead the
    // writer index is so tail can speed up if backlog is too big.
    Optional<Log> latestWrite = logBuffer.getLatestWrite();
    if (!latestWrite.isPresent()) {
      moreWork = Optional.absent();
      return new ForwardResult();
    }

    // the index that was written by previous tail acknowledgement
    long currentReadIndex = getReadIndex();
    Optional<Log> currentLog = logBuffer.get(currentReadIndex);
    if (!currentLog.isPresent()) {
      moreWork = Optional.absent();
      return new ForwardResult();
    }

    // pick the next log by trying to select fixed chunk period
    long fixedFrom = fix(currentLog.get().getTimestamp());
    long fixedTo = fixedFrom + chunkMs - 1;
    // do not process ahead of time, meaning tail will not try process
    // logs until the chunkMs have passed since the present.
    if (fixedTo > System.currentTimeMillis()) {
      moreWork = Optional.absent();
      return new ForwardResult();
    }
    Logs<T> logs;
    if (moreWork.isPresent()) {
      logs = logBuffer.selectForwardPeriod(type, moreWork.get().getIndex(), fixedFrom, fixedTo);
    } else {
      logs = logBuffer.selectPeriod(type, fixedFrom, fixedTo);
    }
    logger.log(Level.FINE, format.format(new Date(fixedFrom)) + " " + format.format(new Date(fixedTo)));
    // don't call tail if there are no logs
    if (logs.isEmpty()) {
      moreWork = Optional.absent();
      return new ForwardResult();
    }
    // prepare the next read index BEFORE we hand over logs to tail
    Log lastRead = logs.getLastLog();
    long lastReadIndex = lastRead.getIndex();
    // prepare result
    ForwardResult result = new ForwardResult();
    moreWork = Optional.absent();
    if (lastRead.getTimestamp() < latestWrite.get().getTimestamp()) {
      // alter the result to indicate that there are already more logs
      // to process after this round have been executed. Hence we can
      // act quickly and process these as fast as possible, if needed.
      this.moreWork = Optional.fromNullable(lastRead);
      result = new ForwardResult(false);
    }
    try {
      // ready to process logs. ignore any exceptions since the LogBuffer
      // will take care of them and retry automatically for us next round.
      // haven't persistent anything to disk yet so tail is fine if it happens
      tail.process(logs);
      // only write/persist last read index if tail was successful
      writeReadIndex(lastReadIndex + 1);
    } catch (Exception e) {
      this.moreWork = Optional.absent();
      result = new ForwardResult();
      System.err.println(e.getMessage());
    }
    return result;
  }

  /**
   * Calculate a fixed period of time aligned with the chunk length.
   */
  private long fix(long timeMs) {
    return timeMs - (timeMs % chunkMs);
  }
}
