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

    public Log(byte[] content) {
        this.content = content;
    }

    public Log(long type, byte[] content) {
        this.type = type;
        this.content = content;
    }

    public long getType() {
        return type;
    }

    public byte[] getContent() {
        return content;
    }

    public int getLength() {
        return 8 + 4 + content.length;
    }

    void write(ExcerptAppender appender) {
        appender.startExcerpt(getLength());
        appender.writeLong(type);
        appender.writeInt(content.length);
        appender.write(content);
        appender.finish();
    }

    static Log read(ExcerptTailer tailer, long index) {
        tailer.index(index);
        long type = tailer.readLong();
        int messageSize = tailer.readInt();
        byte[] message = new byte[messageSize];
        tailer.read(message);
        return new Log(type, message);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Log log = (Log) o;

        if (type != log.type) return false;
        if (!Arrays.equals(content, log.content)) return false;
        if (getLength() != log.getLength()) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (type ^ (type >>> 32));
        result = 31 * result + Arrays.hashCode(content);
        result = 31 * result + getLength();
        return result;
    }
}
