package org.deephacks.logbuffers.cli;


import org.deephacks.logbuffers.*;
import org.deephacks.tools4j.cli.CliCmd;
import org.deephacks.tools4j.cli.CliOption;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class LogBuffersCommand {
  private final SimpleDateFormat SEC_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
  private final SimpleDateFormat MS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

  @CliOption(shortName = "i")
  private String interval = TimeUnit.HOURS.name();

  @CliOption(shortName = "f")
  private String fromTime;

  @CliOption(shortName = "t")
  private String toTime;

  @CliCmd
  public void cat(String path) throws IOException {
    TimeUnit timeUnit = TimeUnit.valueOf(interval.toUpperCase());
    LogBuffer logBuffer = LogBuffer.newBuilder()
      .basePath(path)
      .interval(timeUnit)
      .build();
    long fromMs = format(fromTime, 0);
    long toMs = format(toTime, Long.MAX_VALUE);
    System.out.println(new Date(fromMs) + " ... " + new Date(toMs));
    for (LogRaw log : logBuffer.selectForward(fromMs, toMs)) {
      String timestamp = MS_FORMAT.format(new Date(log.getTimestamp()));
      System.out.println(timestamp + " " + log.getIndex() + " ");
    }
  }

  @CliCmd
  public void tail(String path) throws IOException {
    TimeUnit timeUnit = TimeUnit.valueOf(interval.toUpperCase());
    LogBuffer logBuffer = LogBuffer.newBuilder()
      .basePath(path)
      .interval(timeUnit)
      .build();

    TailSchedule tail = TailSchedule.<LogRaw>builder(new Tail<LogRaw>() {
      @Override
      public void process(Logs<LogRaw> logs) throws RuntimeException {
        for (LogRaw log : logs.get()) {
          String timestamp = MS_FORMAT.format(new Date(log.getTimestamp()));
          System.out.println(timestamp + " " + log.getIndex() + " ");
        }
      }
    }).delay(500, TimeUnit.MILLISECONDS).build();
    logBuffer.forwardWithFixedDelay(tail);
  }

  private long format(String time, long defaultValue) {
    if (time == null || time.trim().equals("")) {
      return defaultValue;
    }
    try {
      return SEC_FORMAT.parse(time).getTime();
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }


}
