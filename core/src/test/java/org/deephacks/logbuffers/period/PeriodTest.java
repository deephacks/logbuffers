package org.deephacks.logbuffers.period;

import org.deephacks.logbuffers.LogBuffer;
import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.LogUtil;
import org.deephacks.logbuffers.Logs;
import org.deephacks.logbuffers.Tail;
import org.deephacks.logbuffers.json.JacksonSerializer;
import org.deephacks.logbuffers.json.JacksonSerializer.A;
import org.deephacks.logbuffers.util.PersistentCounter;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PeriodTest {
  static String dir = "/tmp/logbufferperiod";
  static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  static PersistentCounter counter;
  public static void main(String[] args) throws Exception {
    counter = new PersistentCounter(dir);
    slow_reader();
  }

  public static void slow_reader() throws Exception {
    LogBuffer buffer = new Builder().basePath(dir).addSerializer(new JacksonSerializer()).build();
    new File(dir).mkdirs();
    PeriodicWriter writer = new PeriodicWriter(buffer, 10);
    writer.start();
    TailPeriod tail = new TailPeriod();
    buffer.forwardTimeChunksWithFixedDelay(tail, 1000, 1000, TimeUnit.MILLISECONDS);
    Thread.sleep(3000);
    writer.interrupt();
    Thread.sleep(10000);
    writer = new PeriodicWriter(buffer, 100);
    writer.start();
    Thread.sleep(3000);
    writer.interrupt();
    Thread.sleep(60000);
  }

  public static class TailPeriod implements Tail<A> {

    @Override
    public void process(Logs<A> logs) {
      if (new Random().nextBoolean()) {
        throw new RuntimeException("random failure");
      }
      ArrayList<String> vals = new ArrayList<>();
      for (A a : logs.get()) {
        vals.add(a.timeAndValue());
      }
      System.out.println(vals);
    }
  }


  public static class PeriodicWriter extends Thread {
    private LogBuffer buffer;
    private long maxRandomDelayMs;


    public PeriodicWriter(LogBuffer buffer, long maxRandomDelayMs) throws Exception {
      this.buffer = buffer;
      this.maxRandomDelayMs = maxRandomDelayMs;
    }

    @Override
    public void run() {
      while (true) {
        try {
          A a = new A(counter.getAndIncrement());
          System.out.println("W: " + a.timeAndValue());
          buffer.write(a);
          LogUtil.sleep(maxRandomDelayMs);
        } catch (InterruptedException e) {
          return;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
