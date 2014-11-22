package org.deephacks.logbuffers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.deephacks.logbuffers.Guavas.checkArgument;

public class RollingRanges {
  static SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  static SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-z");
  static SimpleDateFormat MINUTE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-z");
  static SimpleDateFormat SECOND_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-z");
  static SimpleDateFormat MS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS-z");
  static HashMap<String, String> AVAILABLE_TIME_ZONES = new HashMap<>();
  static {
    for (String zone : TimeZone.getAvailableIDs()) {
      AVAILABLE_TIME_ZONES.put(zone, null);
    }
  }
  /** number of events allowed to occur same millisecond */
  public static final int MAX_INDEX_PER_MS = 1000000;
  private long indexesPerInterval = 0;
  private final long interval;

  private final SimpleDateFormat format;
  private final TimeZone defaultTimeZone = TimeZone.getTimeZone("GMT");

  private RollingRanges(TimeUnit unit, SimpleDateFormat format) {
    this.interval = unit.toMillis(1);
    this.indexesPerInterval = interval * MAX_INDEX_PER_MS;
    this.format = format;
    this.format.setTimeZone(defaultTimeZone);
  }

  /**
   * Try create DateRanges from a fromTime formatted string.
   */
  public static RollingRanges tryCreate(String timeFormat) {
    String[] split = timeFormat.split("-");
    if (split.length == 3 && canParse(timeFormat, DAY_FORMAT)) {
      return daily();
    } else if (split.length == 4) {

    } else if (split.length == 5 && validTimeZone(split[split.length - 1])
      && canParse(timeFormat, HOUR_FORMAT)) {
      return hourly();
    } else if (split.length == 6 && validTimeZone(split[split.length - 1])
      && canParse(timeFormat, MINUTE_FORMAT)) {
      return minutely();
    } else if (split.length == 7 && validTimeZone(split[split.length - 1])
      && canParse(timeFormat, SECOND_FORMAT)) {
      return secondly();
    }
    throw new IllegalArgumentException("Do not recognize fromTime format " + timeFormat);
  }

  private static boolean validTimeZone(String s) {
    return AVAILABLE_TIME_ZONES.containsKey(s);
  }

  private static boolean canParse(String timeFormat, SimpleDateFormat simpleDateFormat) {
    try {
      simpleDateFormat.parse(timeFormat);
      return true;
    } catch (ParseException e) {
      return false;
    }
  }

  /**
   * Seconds.
   */
  public static RollingRanges secondly() {
    return new RollingRanges(TimeUnit.SECONDS, SECOND_FORMAT);
  }

  /**
   * Minutes.
   */
  public static RollingRanges minutely() {
    return new RollingRanges(TimeUnit.MINUTES, MINUTE_FORMAT);
  }

  /**
   * Hours.
   */
  public static RollingRanges hourly() {
    return new RollingRanges(TimeUnit.HOURS, HOUR_FORMAT);
  }

  /**
   * Days.
   */
  public static RollingRanges daily() {
    return new RollingRanges(TimeUnit.DAYS, DAY_FORMAT);
  }

  /**
   * Format fromTime according to the interval of this date range.
   */
  public String formatTime(long timestamp) {
    return format.format(new Date(timestamp));
  }

  /**
   * Get the start and stop index that belong to a certain fromTime.
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
    if (time > Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return (time / interval) * indexesPerInterval;
  }

  /**
   * Return the start fromTime for a certain interval.
   */
  public long startTimeForTime(long time) {
    return startTimeForIndex(startIndexForTime(time));
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
   * Get the last index of a certain interval.
   */
  public long stopIndexForIndex(long index) {
    return nextStartIndexForIndex(index) - 1;
  }

  /**
   * Get the next start index for a certain index.
   */
  public long nextStartIndexForIndex(long index) {
    if (index == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return startIndexForIndex(index) + indexesPerInterval;
  }

  /**
   * Get the start fromTime of certain index.
   */
  public long startTimeForIndex(long index) {
    long firstIndexOfInterval = startIndexForIndex(index);
    return ((firstIndexOfInterval / indexesPerInterval) * interval);
  }

  /**
   * Get the stop fromTime of certain index.
   */
  public long stopTimeForIndex(long index) {
    return startTimeForIndex(index) + interval - 1;
  }

  public long stopTimeForTime(long time) {
    return  startTimeForTime(time) + interval - 1;
  }

  public Range indexRange(Range range) {
    long startIndex = startIndexForIndex(range.start());
    return Range.closed(startIndex, startIndex + indexesPerInterval - 1);
  }

  public Range timeRange(Range range) {
    long startTime = startTimeForTime(range.start());
    return Range.closed(startTime, startTime + interval - 1);
  }

  public Range toIndexRange(Range timeRange) {
    long startIndex = startIndexForTime(timeRange.start());
    return Range.closed(startIndex, startIndex + indexesPerInterval - 1);
  }

  public Range toTimeRange(Range indexRange) {
    long startTime = startTimeForIndex(indexRange.start());
    return Range.closed(startTime, startTime + interval - 1);
  }

  public Range indexRange(String timeFormat) {
    long startIndex = startIndex(timeFormat);
    return Range.closed(startIndex, startIndex + indexesPerInterval - 1);
  }

  public Range timeRange(String timeFormat) {
    long startIndex = startIndex(timeFormat);
    long startTime = startTimeForIndex(startIndex);
    return Range.closed(startTime, startTime + interval - 1);
  }

  public Range nextIndexRange(Range range) {
    long start = startIndexForIndex(range.start()) + indexesPerInterval;
    return Range.closed(start, start + indexesPerInterval - 1);
  }

  public Range nextTimeRange(Range range) {
    long start = startTimeForTime(range.start()) + interval;
    return Range.closed(start, start + interval - 1);
  }

  /**
   * Get the formatted fromTime of a certain index.
   */
  public String startTimeFormatForIndex(long index) {
    long time = startTimeForIndex(index);
    Date date = new Date(time);
    return format.format(date);
  }
  /*
  public Range rangeFor(String timeFormat) {
    long startIndex = startIndex(timeFormat);
    return Range. (startIndex, this);
  }
  */

  public long indexOffset(long startTime) {
    return startIndexForTime(startTime);
  }
}
