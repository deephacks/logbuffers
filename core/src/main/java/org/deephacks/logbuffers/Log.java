package org.deephacks.logbuffers;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.util.Arrays;

/**
 * A raw log.
 */
public final class Log {

  /** default type if no type is specified */
  public static long DEFAULT_TYPE = 1;

  /** indicating the binary content format to serializers */
  private long type = DEFAULT_TYPE;

  /** raw log content */
  private final byte[] content;

  /** timestamp when the log was created */
  private final long timestamp;

  /** unique sequential index number (position/offset) */
  private long index = -1;

  Log(Long type, byte[] content, long timestamp, long index) {
    this.type = type;
    this.content = content;
    this.timestamp = timestamp;
    this.index = index;
  }

  Log(byte[] content) {
    this(DEFAULT_TYPE, content, System.currentTimeMillis(), -1);
  }

  Log(long type, byte[] content) {
    this(type, content, System.currentTimeMillis(), -1);
  }

  Log(Log log, long index) {
    this.timestamp = log.timestamp;
    this.content = log.content;
    this.type = log.type;
    this.index = index;
  }

  public long getType() {
    return type;
  }

  /**
   * @return binary log content
   */
  public byte[] getContent() {
    return content;
  }

  /**
   * @return when the log was created
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return unique sequential index number (position/offset) that
   * uniquely identifies this log in a specific buffer
   */
  public long getIndex() {
    return index;
  }

  /**
   * @return timestamp + type + size + content
   */
  public int getLength() {
    return 8 + 8 + 4 + content.length;
  }

  void write(ExcerptAppender appender) {
    appender.startExcerpt(getLength());
    appender.writeLong(getTimestamp());
    appender.writeLong(type);
    appender.writeInt(content.length);
    appender.write(content);
    appender.finish();
  }

  static Optional<Log> read(ExcerptTailer tailer, long index) {
    Preconditions.checkArgument(index >= 0, "index must be positive");
    if (!tailer.index(index)) {
      return Optional.absent();
    }
    long nanoTimestamp = tailer.readLong();
    long type = tailer.readLong();
    int messageSize = tailer.readInt();
    byte[] message = new byte[messageSize];
    tailer.read(message);
    return Optional.fromNullable(new Log(type, message, nanoTimestamp, index));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Log log = (Log) o;

    if (index != log.index) return false;
    if (timestamp != log.timestamp) return false;
    if (type != log.type) return false;
    if (!Arrays.equals(content, log.content)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (type ^ (type >>> 32));
    result = 31 * result + (content != null ? Arrays.hashCode(content) : 0);
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + (int) (index ^ (index >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "Log{" +
            "index=" + index +
            ", timestamp=" + timestamp +
            ", type=" + type +
            ", content=" + Arrays.toString(content) +
            '}';
  }
}
