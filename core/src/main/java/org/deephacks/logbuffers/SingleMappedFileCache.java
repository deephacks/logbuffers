package org.deephacks.logbuffers;

import net.openhft.lang.io.MappedFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

class SingleMappedFileCache {
  private static final AtomicLong totalWait = new AtomicLong();
  private final FileChannel fileChannel;
  private final int blockSize;
  private long lastIndex = Long.MIN_VALUE;
  private
  MappedByteBuffer lastMBB = null;

  public SingleMappedFileCache(String basePath, int blockSize) throws FileNotFoundException {
    this(new File(basePath), blockSize);
  }

  public SingleMappedFileCache(File basePath, int blockSize) throws FileNotFoundException {
    this.blockSize = blockSize;
    fileChannel = new RandomAccessFile(basePath, "rw").getChannel();
  }


  public MappedByteBuffer acquireBuffer(long index, boolean prefetch) {
    if (index == lastIndex)
      return lastMBB;
    long start = System.nanoTime();
    MappedByteBuffer mappedByteBuffer;
    try {
      mappedByteBuffer = getMap(fileChannel, index * blockSize, blockSize);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    lastIndex = index;
    lastMBB = mappedByteBuffer;

    long time = (System.nanoTime() - start);
    if (index > 0)
      totalWait.addAndGet(time);
//            System.out.println("Took " + time + " us to obtain a data chunk");
    return mappedByteBuffer;
  }

  public long size() {
    try {
      return fileChannel.size();
    } catch (IOException e) {
      return 0;
    }
  }

  public void close() {
    try {
      fileChannel.close();
    } catch (IOException ignored) {
    }
  }

  private static MappedByteBuffer getMap(FileChannel fileChannel, long start, int size) throws IOException {
    return MappedFile.getMap(fileChannel, start, size);
  }
}