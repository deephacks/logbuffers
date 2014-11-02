package org.deephacks.logbuffers;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DateRangesTest {
  public static int MAX_INDEX_PER_MS = 1000;

  @Test
  public void testDaily() {
    DateRanges range = DateRanges.daily();
    assertInterval(range, TimeUnit.DAYS, 0, "1970-01-01", "1970-01-02");
  }

  @Test
  public void testHourly() {
    DateRanges range = DateRanges.hourly();
    assertInterval(range, TimeUnit.HOURS, 0, "1970-01-01-00-GMT", "1970-01-01-01-GMT");
  }

  @Test
  public void testMinutely() {
    DateRanges range = DateRanges.minutely();
    assertInterval(range, TimeUnit.MINUTES, 0, "1970-01-01-00-00-GMT", "1970-01-01-00-01-GMT");
  }

  @Test
  public void testSecondly() {
    DateRanges range = DateRanges.secondly();
    assertInterval(range, TimeUnit.SECONDS, 0, "1970-01-01-00-00-00-GMT", "1970-01-01-00-00-01-GMT");
  }

  private void assertInterval(DateRanges range, TimeUnit unit, long time, String format1, String format2) {
    long indexPerInterval = MAX_INDEX_PER_MS * unit.toMillis(1) - 1;
    // current
    long time1 = 0;
    long[] index = range.startStopIndexForTime(time1);
    long expectedToIndex = indexPerInterval;
    assertThat(index[0], is(0L));
    assertThat(index[1], is(expectedToIndex));
    assertThat(range.formatTime(time1), is(format1));
    long t1 = range.startTimeForIndex(index[0]);
    long t2 = range.startTimeForIndex(index[1]);
    assertThat(t1, is(time1));
    assertThat(t1, is(t2));
    assertThat(range.startTimeFormatForIndex(index[0]), is(format1));
    assertThat(range.startTimeFormatForIndex(index[1]), is(format1));
    assertThat(range.startTimeFormatForIndex(index[1] + 1), is(format2));

    // next
    long time2 = 0 + unit.toMillis(1);
    index = range.startStopIndexForTime(time2);
    assertThat(index[0], is(expectedToIndex + 1));
    assertThat(index[1], is(index[0] + indexPerInterval));
    t1 = range.startTimeForIndex(index[0]);
    t2 = range.startTimeForIndex(index[1]);
    assertThat(t1, is(time2));
    assertThat(t1, is(t2));
    assertThat(range.startTimeFormatForIndex(index[0]), is(format2));
    assertThat(range.startTimeFormatForIndex(index[1]), is(format2));
  }

}
