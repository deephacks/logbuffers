package org.deephacks.logbuffers.cli;

import org.deephacks.logbuffers.LogBuffer;
import org.deephacks.logbuffers.Query;
import org.deephacks.tools4j.cli.CliCmd;
import org.deephacks.tools4j.cli.CliOption;

import java.io.IOException;

import static org.deephacks.logbuffers.cli.Util.createLogBuffer;

public class Cat {

  @CliOption(shortName = "i")
  private String interval;

  @CliOption(shortName = "f")
  private String fromTime;

  @CliOption(shortName = "t")
  private String toTime;

  @CliCmd
  public void cat(String path) throws IOException {
    LogBuffer logBuffer = createLogBuffer(path, interval);
    long fromMs = Util.parseDate(fromTime, 0);
    long toMs = Util.parseDate(toTime, Long.MAX_VALUE);
    System.out.println(Util.format(fromMs) + " ... " + Util.format(toMs));
    logBuffer.find(Query.closedTime(fromMs, toMs)).stream().forEach(log -> {
      String value = Util.formatStringValue(log);
      System.out.println(value);
    });
  }
}
