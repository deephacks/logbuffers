package org.deephacks.logbuffers;

import com.google.common.primitives.Longs;
import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.util.JvmCrasher;
import org.deephacks.logbuffers.util.PersistentCounter;

import java.io.File;
import java.util.List;
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
    LogBuffer buffer = new Builder().basePath(DIR).build();
    Tailer tailer = new Tailer();
    new Writer(buffer).start();
    Thread.sleep(500);
    buffer.forwardWithFixedDelay(tailer, 10, TimeUnit.MILLISECONDS);
    new ChaosMonkey().start();
    Thread.sleep(30000);
  }

  public static class Tailer implements Tail<Log> {

    @Override
    public void process(List<Log> logs) {
      StringBuilder sb = new StringBuilder();
      for (Log l : logs) {
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
