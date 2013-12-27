package org.deephacks.logbuffers;

import net.openhft.chronicle.SingleMappedFileCache;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PersistentCounter {
    private ByteBuffer buffer;

    public PersistentCounter() throws FileNotFoundException {
        SingleMappedFileCache numberCache = new SingleMappedFileCache("/tmp/logbuffer/numbers", 8);
        buffer = numberCache.acquireBuffer(0, false).order(ByteOrder.nativeOrder());
    }

    public long getAndIncrement() {
        long value = buffer.getLong(0);
        buffer.putLong(0, value + 1);
        return value;
    }

}
