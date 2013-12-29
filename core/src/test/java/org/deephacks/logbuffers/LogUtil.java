package org.deephacks.logbuffers;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class LogUtil {
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

  public static A randomA(long val) {
    return new A(UUID.randomUUID().toString(), val);
  }

  public static B randomB(long val) {
    return new B(UUID.randomUUID().toString(), val);
  }

  public static Log randomCachedItem() {
    return randomLogs.get(Math.abs(new Random().nextInt()) % randomLogs.size());
  }

  public static class TailA implements Tail<A> {

    public List<A> logs = new ArrayList<>();

    @Override
    public void process(List<A> logs) {
      this.logs.addAll(logs);
    }
  }

  public static class TailB implements Tail<B> {

    public List<B> logs = new ArrayList<>();

    @Override
    public void process(List<B> logs) {
      this.logs.addAll(logs);
    }
  }

  public static class TailLog implements Tail<Log> {

    public List<Log> logs = new ArrayList<>();

    @Override
    public void process(List<Log> logs) {
      this.logs.addAll(logs);
    }
  }


  public static class A {
    private String str;
    private Long val;

    private A() {

    }

    public A(String str, Long val) {
      this.str = str;
      this.val = val;
    }

    public String getStr() {
      return str;
    }

    public Long getVal() {
      return val;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      A a = (A) o;

      if (str != null ? !str.equals(a.str) : a.str != null) return false;
      if (val != null ? !val.equals(a.val) : a.val != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = str != null ? str.hashCode() : 0;
      result = 31 * result + (val != null ? val.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "A{" +
              "str='" + str + '\'' +
              ", val=" + val +
              '}';
    }
  }


  public static class B {
    private String str;
    private Long val;

    private B() {

    }

    public B(String str, Long val) {
      this.str = str;
      this.val = val;
    }

    public String getStr() {
      return str;
    }

    public Long getVal() {
      return val;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      B b = (B) o;

      if (str != null ? !str.equals(b.str) : b.str != null) return false;
      if (val != null ? !val.equals(b.val) : b.val != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = str != null ? str.hashCode() : 0;
      result = 31 * result + (val != null ? val.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "B{" +
              "str='" + str + '\'' +
              ", val=" + val +
              '}';
    }
  }
}
