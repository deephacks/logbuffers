package org.deephacks.logbuffers;

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
  private final long nanoTimestamp;

  /** unique sequential index number (position/offset) */
  private long index = -1;

  Log(Long type, byte[] content, long nanoTimestamp, long index) {
    this.type = type;
    this.content = content;
    this.nanoTimestamp = nanoTimestamp;
    this.index = index;
  }

  Log(byte[] content) {
    this(DEFAULT_TYPE, content, System.nanoTime(), -1);
  }

  Log(long type, byte[] content) {
    this(type, content, System.nanoTime(), -1);
  }

  Log(Log log, long index) {
    this.nanoTimestamp = log.nanoTimestamp;
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
  public long getNanoTimestamp() {
    return nanoTimestamp;
  }

  /**
   * @return unique sequential index number (position/offset) that
   * uniquely identifies this log in a specific buffer
   */
  public long getIndex() {
    return index;
  }

  /**
   * @return nanoTimestamp + type + size + content
   */
  public int getLength() {
    return 8 + 8 + 4 + content.length;
  }

  void write(ExcerptAppender appender) {
    appender.startExcerpt(getLength());
    appender.writeLong(getNanoTimestamp());
    appender.writeLong(type);
    appender.writeInt(content.length);
    appender.write(content);
    appender.finish();
  }

  static Log read(ExcerptTailer tailer, long index) {
    Preconditions.checkArgument(index >= 0, "index must be positive");
    tailer.index(index);
    long nanoTimestamp = tailer.readLong();
    long type = tailer.readLong();
    int messageSize = tailer.readInt();
    byte[] message = new byte[messageSize];
    tailer.read(message);
    return new Log(type, message, nanoTimestamp, index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Log log = (Log) o;

    if (index != log.index) return false;
    if (nanoTimestamp != log.nanoTimestamp) return false;
    if (type != log.type) return false;
    if (!Arrays.equals(content, log.content)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (type ^ (type >>> 32));
    result = 31 * result + (content != null ? Arrays.hashCode(content) : 0);
    result = 31 * result + (int) (nanoTimestamp ^ (nanoTimestamp >>> 32));
    result = 31 * result + (int) (index ^ (index >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "Log{" +
            "index=" + index +
            ", nanoTimestamp=" + nanoTimestamp +
            ", type=" + type +
            ", content=" + Arrays.toString(content) +
            '}';
  }
}
