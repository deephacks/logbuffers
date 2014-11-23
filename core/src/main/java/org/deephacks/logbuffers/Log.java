package org.deephacks.logbuffers;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.deephacks.vals.DirectBuffer;
import org.deephacks.vals.Encodable;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.function.Function;

public class Log implements Comparable<Log> {
  private static byte VERSION = 1;
  private static final byte[] RESERVED_META = new byte[] { VERSION, 0, 0, 0, 0, 0, 0, 0};
  private long index;
  private long localIndex;
  private long timestamp = -1;
  private byte[] content = null;
  private ExcerptTailer tailer;
  private final boolean paddedEntry;

  Log(long timestamp, byte[] content) {
    this.timestamp = timestamp;
    this.content = content;
    this.paddedEntry = false;
  }

  Log(long timestamp, String content) {
    this.timestamp = timestamp;
    this.content = content.getBytes(StandardCharsets.UTF_8);
    this.paddedEntry = false;
  }

  Log(long localIndex, long index, long timestamp, byte[] content) {
    this.timestamp = timestamp;
    this.index = index;
    this.localIndex = localIndex;
    this.content = content;
    this.paddedEntry = false;
  }

  Log(long localIndex, long index, long timestamp, String content) {
    this(localIndex, index, timestamp, content.getBytes(StandardCharsets.UTF_8));
  }

  Log(long localIndex, long index, ExcerptTailer tailer) {
    this.index = index;
    this.localIndex = localIndex;
    this.tailer = tailer;
    this.paddedEntry = false;
  }

  private Log(long localIndex, long index, boolean paddedEntry) {
    this.index = index;
    this.localIndex = localIndex;
    this.paddedEntry = paddedEntry;
  }

  public static Log paddedEntry(long localIndex, long index) {
    return new Log(localIndex, index, true);
  }

  public long getTimestamp() {
    if (timestamp == -1) {
      tailer.index(localIndex);
      this.timestamp = tailer.readLong();
    }
    return timestamp;
  }

  public long getIndex() {
    return index;
  }

  boolean isPaddedEntry() {
    return paddedEntry;
  }

  public byte[] getContent() {
    if (content == null) {
      tailer.index(localIndex);
      int contentSize = tailer.readInt(16);
      tailer.position(20);
      content = new byte[contentSize];
      tailer.read(content);
    }
    return content;
  }

  public <T extends Encodable> T getVal(Function<DirectBuffer, T> parseFrom) {
    tailer.index(localIndex);
    int contentSize = tailer.readInt(16);
    tailer.position(20);
    DirectBuffer buffer = new DirectBuffer(tailer.address() + tailer.position(), contentSize);
    return parseFrom.apply(buffer);
  }

  public String getUtf8() {
    return new String(getContent(), StandardCharsets.UTF_8);
  }

  boolean isIn(Query query) {
    if (query.isIndexQuery()) {
      return query.getRange().contains(index);
    } else {
      return query.getRange().contains(getTimestamp());
    }
  }

  boolean greaterThan(Query query) {
    if (query.isIndexQuery()) {
      return index > query.getRange().start();
    } else {
      return getTimestamp() > query.getRange().start();
    }
  }

  private int getLength() {
    return 8 + 8 + 4 + content.length;
  }

  Log write(AppenderHolder holder) {
    long time = getTimestamp();
    long index = holder.getAppenderIndex(time);
    ExcerptAppender appender = holder.getAppender(time);
    appender.startExcerpt(getLength());
    appender.writeLong(time);
    appender.write(RESERVED_META);
    appender.writeInt(content.length);
    appender.write(content);
    appender.finish();
    this.index = index;
    return this;
  }

  Log write(Encodable e, AppenderHolder holder) {
    long time = getTimestamp();
    long index = holder.getAppenderIndex(time);
    ExcerptAppender appender = holder.getAppender(time);
    int contentLength = e.getTotalSize();
    int logLength = 8 + 8 + 4 + contentLength;
    appender.startExcerpt(logLength);
    appender.writeLong(time);
    appender.write(RESERVED_META);
    appender.writeInt(contentLength);
    e.writeTo(new DirectBuffer(appender.address() + 8 + 8 + 4, contentLength), 0);
    appender.position(logLength);
    appender.finish();
    this.index = index;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Log log = (Log) o;

    if (getIndex() != log.getIndex()) return false;
    if (getTimestamp() != log.getTimestamp()) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (getIndex() ^ (getIndex() >>> 32));
    result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "Log{" +
      "timestamp=" + getTimestamp() +
      ", index=" + getIndex() +
      '}';
  }

  public String toStringDebug() {
    return "Log{" +
      "time=" + RollingRanges.SECOND_FORMAT.format(new Date(timestamp)) +
      ", timestamp=" + getTimestamp() +
      ", index=" + getIndex() +
      '}';
  }

  @Override
  public int compareTo(Log o) {
    return Long.compare(getIndex(), o.getIndex());
  }
}