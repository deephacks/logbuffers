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
  public static List<byte[]> randomLogs = new ArrayList<>();
  public static String[] URLS = new String[] {"www.google.com", "www.cloudera.com", "www.apache.org"};
  static {
    for (int i = 0; i < 1000; i++) {
      randomLogs.add(randomLog());
    }
  }

  public static String tmpDir() {
    try {
      return Files.createTempDirectory("logBufferTest-" + UUID.randomUUID().toString()).toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] randomLog() {
    return UUID.randomUUID().toString().getBytes(Charsets.UTF_8);
  }

  public static byte[] randomCachedItem() {
    return randomLogs.get(Math.abs(new Random().nextInt()) % randomLogs.size());
  }

  public static void sleep(long maxRandomDelayMs) throws InterruptedException {
      long sleep = Math.abs(new Random().nextInt() % maxRandomDelayMs);
      Thread.sleep(sleep);
  }

  public static String formatMs(long ms) {
    return format.format(new Date(ms));
  }

  public static String randomUrl() {
    return URLS[Math.abs(new Random().nextInt()) % URLS.length];
  }

}
