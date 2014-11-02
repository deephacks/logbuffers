package org.deephacks.logbuffers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DateRanges {
  static SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  static SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-z");
  static SimpleDateFormat MINUTE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-z");
  static SimpleDateFormat SECOND_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-z");
  static SimpleDateFormat MS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS-z");

  /** number of events allowed to occur same millisecond */
  private static final int MAX_INDEX_PER_MS = 1000;
  private long indexesPerInterval = 0;
  private final long interval;

  private final SimpleDateFormat format;
  private final TimeZone defaultTimeZone = TimeZone.getTimeZone("GMT");

  private DateRanges(TimeUnit unit, SimpleDateFormat format) {
    this.interval = unit.toMillis(1);
    this.indexesPerInterval = interval * MAX_INDEX_PER_MS;
    this.format = format;
    this.format.setTimeZone(defaultTimeZone);
  }

  /**
   * Create a second range.
   */
  public static DateRanges secondly() {
    return new DateRanges(TimeUnit.SECONDS, SECOND_FORMAT);
  }

  /**
   * Create a minute range.
   */
  public static DateRanges minutely() {
    return new DateRanges(TimeUnit.MINUTES, MINUTE_FORMAT);
  }

  /**
   * Create a hour range.
   */
  public static DateRanges hourly() {
    return new DateRanges(TimeUnit.HOURS, HOUR_FORMAT);
  }

  /**
   * Create a daily range.
   */
  public static DateRanges daily() {
    return new DateRanges(TimeUnit.DAYS, DAY_FORMAT);
  }

  /**
   * Format time according to the interval of this date range.
   */
  public String formatTime(long timestamp) {
    return format.format(new Date(timestamp));
  }

  /**
   * Get the start and stop index that belong to a certain time.
   */
  public long[] startStopIndexForTime(long time) {
    return new long[] {startIndexForTime(time), stopIndexForTime(time)};
  }

  /**
   * Get the last index that belong to a certain period
   */
  public long stopIndexForTime(long time) {
    return ((time + interval) / interval) * indexesPerInterval - 1;
  }

  /**
   * Get the first index that belong to a certain period
   */
  public long startIndexForTime(long time) {
    return (time / interval) * indexesPerInterval;
  }

  /**
   * Get the first index that belong to the formatted period
   */
  public long startIndex(String timeFormat) {
    try {
      Date date = format.parse(timeFormat);
      return startIndexForTime(date.getTime());
    } catch (ParseException e) {
      throw new IllegalArgumentException("Format not recognized " + timeFormat);
    }
  }

  /**
   * Get the first index of a certain interval.
   */
  public long startIndexForIndex(long index) {
    return index - (index % indexesPerInterval);
  }

  /**
   * Get the next start index for a certain index.
   */
  public long nextStartIndexForIndex(long index) {
    return startIndexForIndex(index) + indexesPerInterval;
  }

  /**
   * Get the start time of certain index.
   */
  public long startTimeForIndex(long index) {
    long firstIndexOfInterval = startIndexForIndex(index);
    return ((firstIndexOfInterval / indexesPerInterval) * interval);
  }

  /**
   * Get the stop time of certain index.
   */
  public long stopTimeForIndex(long index) {
    return startTimeForIndex(index) + interval;
  }

  /**
   * Get the formatted time of a certain index.
   */
  public String startTimeFormatForIndex(long index) {
    long time = startTimeForIndex(index);
    Date date = new Date(time);
    return format.format(date);
  }

}