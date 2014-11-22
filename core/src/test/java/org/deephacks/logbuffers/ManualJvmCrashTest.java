package org.deephacks.logbuffers;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This is a manual test that randomly force the JVM to crash and core dump and
 * I have not been able to make a buffer loose any logs during multiple restarts.
 */
public class ManualJvmCrashTest {
  private static String DIR = "/tmp/logbuffer/ManualJvmCrashTest";

  public static void main(String[] args) throws Exception {
    new File(DIR).mkdirs();
    LogBuffer buffer = LogBuffer.newBuilder().secondly().basePath(DIR).build();
    RawLogTail tail = new RawLogTail();
    new Writer(buffer).start();
    Thread.sleep(500);
    TailSchedule schedule = TailSchedule.builder(tail).delay(10, TimeUnit.MILLISECONDS).build();
    buffer.forwardWithFixedDelay(schedule);
    new ChaosMonkey().start();
    Thread.sleep(30000);
  }

  public static class RawLogTail implements Tail {

    @Override
    public void process(Logs logs) {
      StringBuilder sb = new StringBuilder();
      Object collect = logs.stream().map(l -> String.valueOf(l.getIndex())).collect(Collectors.joining(","));
      System.out.println(collect);
    }
  }

  public static class Writer extends Thread {
    private LogBuffer buffer;
    private PersistentCounter counter;

    public Writer(LogBuffer buffer) throws Exception {
      this.buffer = buffer;
      this.counter = new PersistentCounter();
    }

    @Override
    public void run() {
      while (true) {
        try {
          buffer.write(LogUtil.toBytes(counter.getAndIncrement()));
          Thread.sleep(1);
        } catch (Exception e) {

        }
      }
    }
  }

  public static class ChaosMonkey extends Thread {

    @Override
    public void run() {
      try {
        Thread.sleep(Math.abs(new Random().nextInt()) % 10000);
        System.out.println("Good night");
        JvmCrasher.crashJvm();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
