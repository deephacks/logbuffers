package org.deephacks.logbuffers;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

public class Log {
    public static long DEFAULT_TYPE = Long.MIN_VALUE;
    private long type = DEFAULT_TYPE;
    private final byte[] content;
    private Object length;

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
}
