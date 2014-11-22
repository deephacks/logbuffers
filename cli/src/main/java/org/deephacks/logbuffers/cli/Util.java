package org.deephacks.logbuffers.cli;

import org.deephacks.logbuffers.Log;
import org.deephacks.logbuffers.LogBuffer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Util {
  private static final SimpleDateFormat SEC_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
  private static final SimpleDateFormat MS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

  public static LogBuffer createLogBuffer(String path, String interval) throws IOException {
    LogBuffer logBuffer;
    if (interval == null || interval.trim().isEmpty()) {
      return LogBuffer.newBuilder()
        .basePath(path)
        .build();
    } else {
      TimeUnit timeUnit = TimeUnit.valueOf(interval.toUpperCase());
      return LogBuffer.newBuilder()
        .basePath(path)
        .interval(timeUnit)
        .build();
    }
  }

  public static long parseDate(String time, long defaultValue) {
    if (time == null || time.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return SEC_FORMAT.parse(time).getTime();
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static String formatStringValue(Log log) {
    String timestamp = MS_FORMAT.format(new Date(log.getTimestamp()));
    String contents = log.getUtf8();
    return timestamp + " " + log.getIndex() + " " + contents;
  }

  public static String format(long fromMs) {
    return MS_FORMAT.format(new Date(fromMs));
  }
}
