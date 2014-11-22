package org.deephacks.logbuffers.cli;

import org.deephacks.logbuffers.*;
import org.deephacks.tools4j.cli.CliCmd;
import org.deephacks.tools4j.cli.CliOption;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.deephacks.logbuffers.cli.Util.createLogBuffer;

public class Tail {
  @CliOption(shortName = "i")
  private String interval;

  @CliOption(shortName = "f")
  private String fromTime;

  private ConcurrentLinkedQueue<Log> printQueue = new ConcurrentLinkedQueue<>();

  @CliCmd
  public void tail(String path) throws IOException {
    LogBuffer logBuffer = createLogBuffer(path, interval);
    TailSchedule tail = TailSchedule
      .builder(logs -> logs.stream().forEach(l -> printQueue.add(l)))
      .startTime(Util.parseDate(fromTime, System.currentTimeMillis()))
      .delay(10, TimeUnit.MILLISECONDS).build();
    new Thread(() -> {
      while(true) {
        Log log = printQueue.poll();
        if (log != null) {
          String value = Util.formatStringValue(log);
          System.out.println(value);
        }
      }
    }).start();
    logBuffer.forwardWithFixedDelay(tail);
  }
}
