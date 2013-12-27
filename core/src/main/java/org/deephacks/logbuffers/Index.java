package org.deephacks.logbuffers;

import net.openhft.chronicle.SingleMappedFileCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

class Index {
    private SingleMappedFileCache fileCache;
    private ByteBuffer buffer;
    private Mark mark;

    Index(String filePath) throws IOException {
        fileCache = new SingleMappedFileCache(filePath, 16);
        buffer = fileCache.acquireBuffer(0, false);
    }

    synchronized void writeIndex(long index) throws IOException {
        buffer.putLong(0, index);
    }

    synchronized long getIndex() throws IOException {
        long value = buffer.getLong(0);
        return value;
    }

    synchronized void writeTimestamp(long timestamp) {
        buffer.putLong(8, timestamp);
    }

    synchronized long getTimestamp() throws IOException {
        long value = buffer.getLong(8);
        return value;
    }

    void close() throws IOException {
    }

    Mark mark() throws IOException {
        mark = new Mark(getIndex(), getTimestamp());
        return mark;
    }

    Mark getMark() throws IOException {
        return mark;
    }

    void rewind() throws IOException {
        writeIndex(mark.getIndex());
        writeTimestamp(mark.getTimestamp());
    }

    boolean isReadable(long writeIndex) throws IOException {
        long index = getIndex();
        return index < writeIndex;
    }

    public long getAndIncrement() throws IOException {
        long index = getIndex();
        writeIndex(index + 1);
        writeTimestamp(System.currentTimeMillis());
        return index;
    }

    static class Mark {
        private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        long timestamp;
        long index;

        private Mark(long index, long timestamp) {
            this.timestamp = timestamp;
            this.index = index;
        }

        private long getTimestamp() {
            return timestamp;
        }

        private long getIndex() {
            return index;
        }

        @Override
        public String toString() {
            return "Mark{" +
                    "index=" + index +
                    ", timestamp=" + timestamp +
                    ", time=" + FORMAT.format(new Date(timestamp)) +
                    '}';
        }
    }
}
