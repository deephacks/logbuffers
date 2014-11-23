package org.deephacks.logbuffers.cli;

import org.deephacks.logbuffers.*;
import org.deephacks.logbuffers.Tail;
import org.deephacks.tools4j.cli.CliCmd;
import org.deephacks.tools4j.cli.CliOption;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.deephacks.logbuffers.cli.Util.createLogBuffer;

public class Test {

  public static void main(String[] args) throws Exception {
    new Test().seqCheck(null);
  }

  @CliOption(shortName = "i")
  private String interval;

  @CliOption(shortName = "s")
  private Integer speed = 10;

  @CliCmd
  public void seq(String path) throws Exception {
    LogBuffer logBuffer = createLogBuffer(path, interval);
    AtomicLong increment = new AtomicLong();
    while (true) {
      int sleep = Math.abs(new Random().nextInt()) % speed;
      if (sleep == 0) {
        Thread.sleep(1);
      }
      Log log = logBuffer.write(String.valueOf(increment.incrementAndGet()));
      if (increment.get() % 100000 == 0) {
        System.out.println(Util.formatStringValue(log));
      }
    }
  }

  private static Log previous = null;
  private static Log current = null;

  @CliCmd
  public void seqCheck(String path) throws Exception {
    LogBuffer logBuffer = createLogBuffer(path, interval);
    TailSchedule tail = TailSchedule.builder(logs -> logs.stream().forEach(log -> {
      current = log;
      long currentSeq = Long.parseLong(current.getUtf8());
      if (previous == null) {
        previous = current;
      } else {
        // 1 means the writer have restarted so don't check the sequence number
        if (currentSeq != 1L && Long.parseLong(previous.getUtf8()) + 1 != currentSeq) {
          exit(logBuffer);
        }
        if (previous.getIndex() >= current.getIndex()) {
          exit(logBuffer);
        }
        previous = current;
      }
      if (currentSeq % 100000 == 0) {
        System.out.println(Util.formatStringValue(log));
      }
    }))
      .startTime(0)
      .delay(100, TimeUnit.MILLISECONDS)
      .build();
    logBuffer.forwardWithFixedDelay(tail);
  }

  @CliCmd
  public void count(String path) throws Exception {
    LogBuffer logBuffer = createLogBuffer(path, interval);
    for (int i = 0; i < 3; i++) {
      long now = System.currentTimeMillis();
      System.out.print(logBuffer.parallel().stream().count() + " took " );
      System.out.println(System.currentTimeMillis() - now + "ms");
    }
  }

  private void exit(LogBuffer logBuffer) {
    System.err.println("curr " + current.getUtf8() + " " + current);
    System.err.println("prev" + previous.getUtf8() + " " + previous);
    try {
      logBuffer.close();
    } catch (IOException e) {
    }
    throw new IllegalStateException("");
  }
}
