package org.deephacks.logbuffers;


public class Range {
  private final long start;
  private final long stop;

  private Range(long start, long stop) {
    this.start = start;
    this.stop = stop;
  }

  /**
   * greater than or equal to start and less than or equal to stop
   */
  public static Range closed(long start, long stop) {
    return new Range(start, stop);
  }

  /**
   * less than or equal to
   */
  public static Range atMost(long value) {
    return new Range(Long.MIN_VALUE, value);
  }

  /**
   * greater than or equal to
   */
  public static Range atLeast(long value) {
    return new Range(value, Long.MAX_VALUE);
  }

  public long start() {
    return start;
  }

  public long stop() {
    return stop;
  }

  public boolean isConnected(Range range) {
    return range.start <= stop && start <= range.stop;
  }
  /*
      public Range firstRollingRange() {
        if (startTime == Long.MIN_VALUE) {
          return Range.index(startIndex, ranges);
        } else {
          return Range.time(startTime, ranges);
        }
      }

      public Range rollNextRange() {
        long index = ranges.nextStartIndexForIndex(startIndex);
        return new Range(index, ranges);
      }
  */
  public boolean contains(long value) {
    return start <= value && stop >= value;
  }

  @Override
  public String toString() {
    return "Range{" +
      "start=" + start +
      ", stop=" + stop +
      '}';
  }
}

