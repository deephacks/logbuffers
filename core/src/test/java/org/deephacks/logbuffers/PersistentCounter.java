package org.deephacks.logbuffers;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PersistentCounter {
  private ByteBuffer buffer;

  public PersistentCounter(String basePath) throws FileNotFoundException {
    File file = new File(basePath);
    file.mkdirs();
    SingleMappedFileCache numberCache = new SingleMappedFileCache(basePath + "/numbers", 8);
    buffer = numberCache.acquireBuffer(0, false).order(ByteOrder.nativeOrder());
  }


  public PersistentCounter() throws FileNotFoundException {
    this("/tmp/logbuffer");
  }

  public long getAndIncrement() {
    long value = buffer.getLong(0);
    buffer.putLong(0, value + 1);
    return value;
  }
}
