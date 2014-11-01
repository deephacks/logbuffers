package org.deephacks.logbuffers;

import com.google.common.primitives.Longs;
import org.deephacks.logbuffers.LogBuffer.Builder;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This is a manual test that randomly force the JVM to crash and core dump and
 * I have not been able to make a buffer loose any logs during multiple restarts.
 */
public class ManualJvmCrashTest {
  private static String DIR = "/tmp/logbuffer/ManualJvmCrashTest";

  public static void main(String[] args) throws Exception {
    new File(DIR).mkdirs();
    LogBuffer buffer = LogBuffer.newBuilder().basePath(DIR).build();
    RawLogTail tail = new RawLogTail();
    new Writer(buffer).start();
    Thread.sleep(500);
    TailSchedule schedule = TailSchedule.builder(tail).delay(10, TimeUnit.MILLISECONDS).build();
    buffer.forwardWithFixedDelay(schedule);
    new ChaosMonkey().start();
    Thread.sleep(30000);
  }

  public static class RawLogTail implements Tail<LogRaw> {

    @Override
    public void process(Logs<LogRaw> logs) {
      StringBuilder sb = new StringBuilder();
      for (LogRaw l : logs.get()) {
        sb.append(l.getIndex()).append(",");
      }
      System.out.println(sb.toString());
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
          buffer.write(Longs.toByteArray(counter.getAndIncrement()));
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
