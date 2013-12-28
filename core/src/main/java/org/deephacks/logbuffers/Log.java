package org.deephacks.logbuffers;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.util.Arrays;

/**
 * A raw log message. Every log message have a type.
 *
 * If no type is specified, the log is treated as a raw binary log.
 */
public class Log {
    public static long DEFAULT_TYPE = 1;
    private long type = DEFAULT_TYPE;
    private final byte[] content;
    private final long timestamp;
    private long index = -1;

    public Log(Long type, byte[] content, long timestamp, long index) {
        this.type = type;
        this.content = content;
        this.timestamp = timestamp;
        this.index = index;
    }

    public Log(byte[] content) {
        this(DEFAULT_TYPE, content, System.currentTimeMillis(), -1);
    }

    public Log(long type, byte[] content) {
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

    public byte[] getContent() {
        return content;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getIndex() {
        return index;
    }

    public int getLength() {
        // timestamp + type + size + content
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

    static Log read(ExcerptTailer tailer, long index) {
        tailer.index(index);
        long timestamp = tailer.readLong();
        long type = tailer.readLong();
        int messageSize = tailer.readInt();
        byte[] message = new byte[messageSize];
        tailer.read(message);
        return new Log(type, message, timestamp, index);
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
