package org.deephacks.logbuffers;

import net.openhft.chronicle.SingleMappedFileCache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Keeps track of the index for a specific reader or writer.
 */
final class Index {
    private SingleMappedFileCache fileCache;
    private ByteBuffer buffer;

    Index(String filePath) throws IOException {
        fileCache = new SingleMappedFileCache(filePath, 16);
        buffer = fileCache.acquireBuffer(0, false);
    }

    synchronized void writeIndex(long index) throws IOException {
        buffer.putLong(0, index);
    }

    synchronized long getIndex() throws IOException {
        return buffer.getLong(0);
    }

    synchronized void writeNanoTimestamp(long nanos) {
        buffer.putLong(8, nanos);
    }

    synchronized long getNanoTimestamp() throws IOException {
        return buffer.getLong(8);
    }

    void close() throws IOException {
        fileCache.close();
    }

    long getAndIncrement() throws IOException {
        long index = getIndex();
        writeIndex(index + 1);
        writeNanoTimestamp(System.nanoTime());
        return index;
    }
}
