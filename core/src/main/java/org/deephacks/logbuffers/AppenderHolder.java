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

  AppenderHolder(String path, DateRanges ranges) {
    long now = System.currentTimeMillis();
    this.ranges = ranges;
    this.basePath = path;
    new File(this.basePath).mkdirs();
    this.stopIndex = ranges.stopIndexForTime(now);
    long startIndex = ranges.startIndexForTime(now);
    String intervalDir = ranges.startTimeFormatForIndex(startIndex);
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
      long startIndex = ranges.startIndexForTime(time);
      if (this.stopIndex < startIndex) {
        this.appender.close();
        this.chronicle.close();
        this.stopIndex = ranges.nextStartIndexForIndex(startIndex) - 1;
        String intervalDir = ranges.startTimeFormatForIndex(startIndex);
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
    long index = ranges.startIndexForTime(time);
    return appender.index() + index;
  }
}
