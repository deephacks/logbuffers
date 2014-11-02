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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.util.Arrays;

import static org.deephacks.logbuffers.TailerHolder.Tailer;

/**
 * A raw log.
 */
public final class LogRaw {
  /** default type if no type is specified */
  public static long DEFAULT_TYPE = 1;

  /** indicating the binary content format to serializers */
  private final long type;

  /** raw log content */
  private final byte[] content;

  /** timestamp when the log was created */
  private final long timestamp;

  /** unique sequential index number (position/offset) */
  private final long index;

  LogRaw(Long type, byte[] content, long timestamp, long index) {
    Preconditions.checkArgument(type != 0, "Type cannot be 0");
    this.type = type;
    this.content = content;
    this.timestamp = timestamp;
    this.index = index;
  }

  LogRaw(byte[] content) {
    this(DEFAULT_TYPE, content, System.currentTimeMillis(), DEFAULT_TYPE);
  }

  LogRaw(long type, byte[] content) {
    this(type, content, System.currentTimeMillis(), DEFAULT_TYPE);
  }

  LogRaw(LogRaw log, long index) {
    this(log.getType(), log.getContent(), log.getTimestamp(), index);
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

  long write(AppenderHolder holder) {
    long time = getTimestamp();
    long index = holder.getAppenderIndex(time);
    ExcerptAppender appender = holder.getAppender(time);
    appender.startExcerpt(getLength());
    appender.writeLong(time);
    appender.writeLong(type);
    appender.writeInt(content.length);
    appender.write(content);
    appender.finish();
    return index;
  }

  static Optional<LogRaw> read(Tailer tailer, long index) {
    long holderIndex = tailer.getHolderIndex(index);
    if (!tailer.tailer.index(holderIndex)) {
      return Optional.absent();
    }
    long timestamp = tailer.tailer.readLong();
    long type = tailer.tailer.readLong();
    int messageSize = tailer.tailer.readInt();
    byte[] message = new byte[messageSize];
    tailer.tailer.read(message);
    if (type == 0) {
      return Optional.absent();
    }
    return Optional.fromNullable(new LogRaw(type, message, timestamp, index));
  }

  static Optional<LogRaw> read(TailerHolder holder, long index) {
    Preconditions.checkArgument(index >= 0, "index must be positive");
    Optional<ExcerptTailer> optional = holder.getExcerptTailerForIndex(index);
    if (!optional.isPresent()) {
      return Optional.absent();
    }
    ExcerptTailer tailer = optional.get();
    long holderIndex = holder.convertToLocalIndex(index);
    if (!tailer.index(holderIndex)) {
      return Optional.absent();
    }
    long timestamp = tailer.readLong();
    long type = tailer.readLong();
    int messageSize = tailer.readInt();
    byte[] message = new byte[messageSize];
    tailer.read(message);
    if (type == 0) {
      return Optional.absent();
    }
    return Optional.fromNullable(new LogRaw(type, message, timestamp, index));
  }

  static Optional<Long> peekTimestamp(Tailer t, long index) {
    Preconditions.checkArgument(index >= 0, "index must be positive");
    ExcerptTailer tailer = t.tailer;
    if (!tailer.index(t.getHolderIndex(index))) {
      return Optional.absent();
    }
    long timestamp = tailer.readLong();
    return Optional.fromNullable(timestamp);
  }

  public static Optional<Long> peekType(TailerHolder holder, long index) {
    Preconditions.checkArgument(index >= 0, "index must be positive");
    Optional<ExcerptTailer> optional = holder.getExcerptTailerForIndex(index);
    if (!optional.isPresent()) {
      return Optional.absent();
    }
    ExcerptTailer tailer = optional.get();
    if (!tailer.index(holder.convertToLocalIndex(index))) {
      return Optional.absent();
    }
    long timestamp = tailer.readLong();
    long type = tailer.readLong();
    if (type == 0) {
      return Optional.absent();
    }
    return Optional.fromNullable(type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LogRaw log = (LogRaw) o;

    if (index != log.index) return false;
    if (timestamp != log.timestamp) return false;
    if (type != log.type) return false;
    if (!Arrays.equals(content, log.content)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (type ^ (type >>> 32));
    result = 31 * result + (int) (type ^ (type >>> 32));
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + (int) (index ^ (index >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "LogRaw{" +
            "index=" + index +
            ", timestamp=" + timestamp +
            ", type=" + type +
            '}';
  }
}
