package org.deephacks.logbuffers;

public class Query {
  private final QueryType type;
  private final Range range;

  Query(QueryType type, Range range){
    this.type = type;
    this.range = range;
  }

  public static Query closedIndex(long start, long stop) {
    return new Query(QueryType.INDEX, Range.closed(start, stop));
  }

  public static Query atLeastIndex(long start) {
    return new Query(QueryType.INDEX, Range.atLeast(start));
  }

  public static Query atMostIndex(long stop) {
    return new Query(QueryType.INDEX, Range.atMost(stop));
  }

  public static Query closedTime(long start, long stop) {
    return new Query(QueryType.TIME, Range.closed(start, stop));
  }

  public static Query atLeastTime(long start) {
    return new Query(QueryType.TIME, Range.atLeast(start));
  }

  public static Query atMostTime(long stop) {
    return new Query(QueryType.TIME, Range.atMost(stop));
  }

  public long start() {
    return range.start();
  }

  public long stop() {
    return range.stop();
  }

  public QueryType type() {
    return type;
  }

  public Range getRange() {
    return range;
  }

  public boolean isTimeQuery() {
    return type == QueryType.TIME;
  }

  public boolean isIndexQuery() {
    return type == QueryType.INDEX;
  }

  public Range nextRange(Range current, RollingRanges ranges) {
    if (type == QueryType.INDEX) {
      return current == null ? ranges.indexRange(range) : ranges.nextIndexRange(current);
    } else {
      return current == null ? ranges.timeRange(range) : ranges.nextTimeRange(current);
    }
  }

  private static enum QueryType {
    INDEX, TIME
  }

  @Override
  public String toString() {
    return "Query." + type + "{" +
       range +
      '}';
  }
}
