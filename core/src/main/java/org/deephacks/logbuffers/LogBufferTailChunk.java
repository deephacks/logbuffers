package org.deephacks.logbuffers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class LogBufferTailChunk<T> extends LogBufferTail<T> {
  private long chunkMs;
  public static SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");

  LogBufferTailChunk(LogBuffer logBuffer, TailChunk<T> tail, long chunkMs) throws IOException {
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
    if (logs.size() < 0) {
      return new ForwardResult();
    }
    tail.process(logs);
    List<T> objects = logs.get();
    T last = objects.get(objects.size() - 1);
    // only write the read index if tail was successful
    Log logRead = logs.get(last);
    long index = logRead.getIndex();
    writeReadIndex(index + 1);

    if (logRead.getTimestamp() < lastWrite.getTimestamp()) {
      return new ForwardResult(false);
    }
    return new ForwardResult();
  }

  public long fix(long timeMs) {
    return timeMs - (timeMs % chunkMs);
  }
}
