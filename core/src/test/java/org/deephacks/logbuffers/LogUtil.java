package org.deephacks.logbuffers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LogUtil extends Dirs {
  public static TreeMap<Long, Log> logs = new TreeMap<>();
  public static TreeMap<Long, Long> lastWritten = new TreeMap<>();

  private LogUtil(Builder builder) {
    super(builder.dirs, builder.ranges);
  }

  static Builder newBuilder(RollingRanges ranges) {
    logs.clear();
    return new Builder(ranges);
  }

  static Builder newBuilder() {
    logs.clear();
    return new Builder(RollingRanges.secondly());
  }

  public static LogBuffer empty() throws IOException {
    return LogUtil.newBuilder(RollingRanges.secondly()).build();
  }

  public static LogBuffer defaults(long time) throws IOException {
    return LogUtil.newBuilder(RollingRanges.secondly())
      .tick(time)
      .add("1")
      .paddedEntry()
      .add("2")
      .tick(1000)
      .add("3")
      .paddedEntry()
      .add("4")
      .tick(3000)
      .add("5")
      .build();
  }

  public static class Builder {
    private long currentTime = 0;
    private RollingRanges ranges;
    private TreeMap<Long, Dir> dirs = new TreeMap<>();
    private TreeMap<Long, AtomicLong> index = new TreeMap<>();

    private Builder(RollingRanges ranges) {
      this.ranges = ranges;
    }

    public Builder tick(long time) {
      this.currentTime = currentTime + time;
      return this;
    }

    public Builder add(String content) {
      long currentIndex = addNext();
      logs.put(currentIndex, new Log(currentIndex, currentIndex, currentTime, content));
      return this;
    }

    public Builder paddedEntry() {
      long currentIndex = addNext();
      logs.put(currentIndex, Log.paddedEntry(currentIndex, currentIndex));
      return this;
    }

    private long addNext() {
      long startIndex = ranges.startIndexForTime(currentTime);
      if (!index.containsKey(startIndex)) {
        index.put(startIndex, new AtomicLong(startIndex));
        File file = new File(ranges.startTimeFormatForIndex(startIndex));
        dirs.put(startIndex, new DirStub(file, ranges));
      }
      long currentIndex = index.get(startIndex).getAndIncrement();
      lastWritten.put(startIndex, currentIndex);
      return currentIndex;
    }

    public LogBuffer build() throws IOException {
      return LogBuffer.newBuilder().ranges(ranges).dirs(new LogUtil(this)).build();
    }
  }

  @Override
  void initialize() {
  }

  public static class DirStub extends Dir {
    DirStub(File basePath, RollingRanges ranges) {
      super(basePath, ranges, null);
    }

    @Override
    public Log getLog(long index) {
      return logs.get(index);
    }

    @Override
    public long getLastWrittenIndex() {
      return lastWritten.get(this.getIndexRange().start());
    }
  }

  public static byte[] randomLog() {
    return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
  }

  private static final File tmpDir = new File(System.getProperty("java.io.tmpdir"), "logBufferTest");

  public static String cleanupTmpDir() {
    if (tmpDir.exists()) {
      deleteRecursive(tmpDir);
    }
    tmpDir.mkdirs();
    return tmpDir.toString();
  }

  private static void deleteRecursive(File path) {
    File[] c = path.listFiles();
    if (c == null) {
      return;
    }
    for (File file : c) {
      if (file.isDirectory()) {
        deleteRecursive(file);
        file.delete();
      } else {
        file.delete();
      }
    }
    path.delete();
  }


  public static LinkedList<Log> write(LogBuffer logBuffer) throws Exception {
    // enough sleep to write thousands of logs
    return write(logBuffer, 1, TimeUnit.SECONDS.toMillis(3));
  }

  public static LinkedList<Log> write(LogBuffer logBuffer, long delay, long duration) throws Exception {
    LinkedList<Log> written = new LinkedList<>();
    long first = System.currentTimeMillis();
    long i = 0;
    long stop = first + duration;
    while (stop > System.currentTimeMillis()) {
      Log log = logBuffer.write(toBytes(i++));
      written.add(log);
      if (delay == 0 && (new Random().nextInt() % 100) == 0) {
        // no sleep is a bit too fast, sometimes causing timing issues in tests.
        // this random delay still produce logs with same timestamp and make
        // tests faster and more reliable.
        Thread.sleep(1);
      } else {
        Thread.sleep(delay);
      }
    }
    System.out.println("Wrote " + written.size());
    return written;
  }

  public static List<Log> writeList(LogBuffer logBuffer, long delay, long duration) throws Exception {
    ArrayList<Log> written = new ArrayList<>();
    long first = System.currentTimeMillis();
    long i = 0;
    long stop = first + duration;
    while (stop > System.currentTimeMillis()) {
      Log log = logBuffer.write(toBytes(i++));
      written.add(log);
      if (delay == 0) {
        //Thread.sleep(1);
      } else {
        Thread.sleep(delay);
      }
    }
    System.out.println("Wrote " + written.size());
    return written;
  }

  public static byte[] toBytes(final int n) {
    byte[] b = new byte[4];
    b[0] = (byte) (n >>> 24);
    b[1] = (byte) (n >>> 16);
    b[2] = (byte) (n >>> 8);
    b[3] = (byte) (n >>> 0);
    return b;
  }

  public static byte[] toBytes(final long n) {
    byte[] b = new byte[8];
    b[0] = (byte) (n >>> 56);
    b[1] = (byte) (n >>> 48);
    b[2] = (byte) (n >>> 40);
    b[3] = (byte) (n >>> 32);
    b[4] = (byte) (n >>> 24);
    b[5] = (byte) (n >>> 16);
    b[6] = (byte) (n >>> 8);
    b[7] = (byte) (n >>> 0);
    return b;
  }

  public static long getLong(byte[] b) {
    if (b.length != 8) {
      System.out.println(Arrays.toString(b));
    }
    return (b[0] & 0xFFL) << 56 | (b[1] & 0xFFL) << 48
      | (b[2] & 0xFFL) << 40 | (b[3] & 0xFFL) << 32
      | (b[4] & 0xFFL) << 24 | (b[5] & 0xFFL) << 16
      | (b[6] & 0xFFL) << 8 | (b[7] & 0xFFL) << 0;

  }
}
