package org.deephacks.logbuffers;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class LogUtil {
  public static SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");
  public static List<Log> randomLogs = new ArrayList<>();

  static {
    for (int i = 0; i < 1000; i++) {
      randomLogs.add(randomLog(i));
    }
  }

  public static String tmpDir() {
    try {
      return Files.createTempDirectory("logBufferTest-" + UUID.randomUUID().toString()).toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Log randomLog(long timestamp) {
    return new Log(Log.DEFAULT_TYPE, UUID.randomUUID().toString().getBytes(Charsets.UTF_8), timestamp, timestamp);
  }

  public static Log randomCachedItem() {
    return randomLogs.get(Math.abs(new Random().nextInt()) % randomLogs.size());
  }

  public static void sleep(long maxRandomDelayMs) throws InterruptedException {
      long sleep = Math.abs(new Random().nextInt() % maxRandomDelayMs);
      Thread.sleep(sleep);
  }

  public static String formatMs(long ms) {
    return format.format(new Date(ms));
  }

}
