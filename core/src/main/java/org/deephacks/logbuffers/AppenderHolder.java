package org.deephacks.logbuffers;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;

import java.io.File;
import java.io.IOException;

class AppenderHolder {
  private final DateRanges ranges;
  private final String basePath;
  private IndexedChronicle chronicle;
  public ExcerptAppender appender;
  private long stopIndex = -1;
  private long appenderIndex;

  AppenderHolder(String path, DateRanges ranges) {
    this.ranges = ranges;
    this.basePath = path;
    new File(this.basePath).mkdirs();
    long startIndex = ranges.startIndex(System.currentTimeMillis());
    String intervalDir = ranges.getStartTimeFormat(startIndex);
    File basePathDir = new File(basePath, intervalDir);
    basePathDir.mkdirs();
    try {
      this.chronicle = new IndexedChronicle(basePathDir.getAbsolutePath() + "/" + intervalDir);
      this.appender = chronicle.createAppender();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  ExcerptAppender getAppender(long time){
    try {
      long startIndex = ranges.startIndex(time);
      long startTime = ranges.getStartTime(startIndex + appender.index());
      if (startTime > time) {
        this.chronicle.close();
        this.chronicle.close();
        String intervalDir = ranges.getStartTimeFormat(startIndex);
        File basePathDir = new File(basePath, intervalDir);
        this.chronicle = new IndexedChronicle(basePathDir.getAbsolutePath() + "/" + intervalDir);
        this.appender = chronicle.createAppender();
      }
      return appender;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    if (chronicle != null) {
      appender.close();
      chronicle.close();
    }
  }

  public long getAppenderIndex(long time) {
    ExcerptAppender appender = getAppender(time);
    long index = ranges.startIndex(time);
    return appender.index() + index;
  }
}
