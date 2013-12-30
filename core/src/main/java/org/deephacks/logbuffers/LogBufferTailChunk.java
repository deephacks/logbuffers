package org.deephacks.logbuffers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Specialized tail that provide logs iteratively in chunks according to a certain period of time.
 */
class LogBufferTailChunk<T> extends LogBufferTail<T> {
  private long chunkMs;
  public static SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");

  LogBufferTailChunk(LogBuffer logBuffer, Tail<T> tail, long chunkMs) throws IOException {
    super(logBuffer, tail);
    this.chunkMs = chunkMs;
  }

  @Override
  ForwardResult forward() throws IOException {
    long currentReadIndex = getReadIndex();
    List<Log> writes = logBuffer.select(logBuffer.getWriteIndex() - 1);
    if (writes.isEmpty()) {
      return new ForwardResult();
    }
    Log lastWrite = writes.get(0);

    List<Log> current = logBuffer.select(currentReadIndex);
    if (current.size() == 0) {
      return new ForwardResult();
    }
    long fixedFrom = fix(current.get(0).getTimestamp());
    long fixedTo = fixedFrom + chunkMs - 1;
    // do not process ahead of time.
    if (fixedTo > System.currentTimeMillis()) {
      return new ForwardResult();
    }
    Logs<T> logs = logBuffer.selectPeriod(type, fixedFrom, fixedTo);
    System.out.println(format.format(new Date(fixedFrom)) + " " + format.format(new Date(fixedTo)));

    // don't call tail if there are no logs
    if (logs.size() < 0) {
      return new ForwardResult();
    }
    // prepare the next read index BEFORE we hand over logs to tail
    Log logRead = getLastLog(logs);
    long lastReadIndex = logRead.getIndex();
    // prepare result
    ForwardResult result = new ForwardResult();
    if (logRead.getTimestamp() < lastWrite.getTimestamp()) {
      // alter the result to indicate that there are already more logs
      // to process after this round have been executed. Hence we can
      // act quickly and process these as fast as possible, if needed.
      result = new ForwardResult(false);
    }
    // ready to process logs. ignore any exceptions since the LogBuffer
    // will take care of them and retry automatically for us next round.
    // haven't persistent anything to disk yet so tail is fine if it happens
    tail.process(logs);
    // only write the read lastReadIndex if tail was successful
    writeReadIndex(lastReadIndex + 1);
    return result;
  }

  private Log getLastLog(Logs<T> logs) {
    List<T> objects = logs.get();
    T last = objects.get(objects.size() - 1);
    return logs.get(last);
  }

  public long fix(long timeMs) {
    return timeMs - (timeMs % chunkMs);
  }
}
