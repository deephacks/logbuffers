package org.deephacks.logbuffers;

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

class AppenderHolder {
  private static final Logger logger = LoggerFactory.getLogger(AppenderHolder.class);
  private final RollingRanges ranges;
  private final File basePath;
  private IndexedChronicle chronicle;
  public ExcerptAppender appender;
  private long stopIndex = -1;
  private final ChronicleConfig config;

  AppenderHolder(File path, Optional<RollingRanges> ranges, long time, ChronicleConfig config) {
    this.config = config;
    this.ranges = ranges.orElse(RollingRanges.hourly());
    this.basePath = path;
    basePath.mkdirs();
    this.stopIndex = this.ranges.stopIndexForTime(time);
    long startIndex = this.ranges.startIndexForTime(time);
    String intervalDir = this.ranges.startTimeFormatForIndex(startIndex);
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
        File basePathDir = new File(basePath, intervalDir + "/" + intervalDir);
        logger.debug("appender {}", basePathDir);
        this.chronicle = new IndexedChronicle(basePathDir.toString(), config);
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
