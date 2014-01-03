package org.deephacks.logbuffers.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.deephacks.logbuffers.LogUtil;
import org.deephacks.logbuffers.Logs;
import org.deephacks.logbuffers.ObjectLogSerializer;
import org.deephacks.logbuffers.Tail;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class JacksonSerializer implements ObjectLogSerializer {

  private BiMap<Long, Class<?>> mapping = HashBiMap.create();

  private ObjectMapper mapper = new ObjectMapper();

  public JacksonSerializer() {
    mapping.put(123L, A.class);
    mapping.put(124L, B.class);
    mapping.put(125L, PageViews.class);
  }

  @Override
  public BiMap<Long, Class<?>> getMapping() {
    return mapping;
  }

  @Override
  public byte[] serialize(Object object) {
    Long type = mapping.inverse().get(object.getClass());
    Preconditions.checkNotNull(type, "No type mapped to class " + object.getClass());
    StringWriter writer = new StringWriter();
    try {
      mapper.writeValue(writer, object);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return writer.toString().getBytes(Charsets.UTF_8);
  }

  @Override
  public Object deserialize(byte[] log, long type) {
    Class<?> cls = mapping.get(type);
    try {
      return mapper.readValue(log, cls);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class A {
    private String str;
    private Long val;
    private long timestamp;

    private A() {
    }

    public A(Long val) {
      this.val = val;
      this.timestamp = System.currentTimeMillis();
    }

    public A(String str, Long val) {
      this.str = str;
      this.val = val;
      this.timestamp = System.currentTimeMillis();
    }

    public String getStr() {
      return str;
    }

    public Long getVal() {
      return val;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public String time() {
      return LogUtil.formatMs(timestamp) ;
    }

    public String timeAndValue() {
      return val + " " + time();
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
      return "PageView{" +
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

  public static class TailA implements Tail<A> {

    public List<A> logs = new ArrayList<>();

    @Override
    public void process(Logs<A> logs) {
      this.logs.addAll(logs.get());
    }
  }

  public static class TailB implements Tail<B> {

    public List<B> logs = new ArrayList<>();

    @Override
    public void process(Logs<B> logs) {
      this.logs.addAll(logs.get());
    }
  }

  public static A randomA(long val) {
    return new A(UUID.randomUUID().toString(), val);
  }

  public static B randomB(long val) {
    return new B(UUID.randomUUID().toString(), val);
  }

  public static class PageViews {
    private static SimpleDateFormat FORMAT = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss:SSS");
    private HashMap<String, Count> counts = new HashMap<>();
    private Long from;
    private Long to;
    private PageViews() {
    }

    public PageViews(Long from, Long to) {
      this.from = from;
      this.to = to;
    }

    public void increment(String url, long increment) {
      Count count = counts.get(url);
      if (count == null) {
        count = new Count(url, increment);
        counts.put(url, count);
      } else {
        count.increment(increment);
        counts.put(url, count);
      }
    }

    public HashMap<String, Count> getCounts() {
      return counts;
    }

    public Long getFrom() {
      return from;
    }

    public Long getTo() {
      return to;
    }

    public long total() {
      long total = 0;
      for (Count count : counts.values()) {
        total += count.getIncrement();
      }
      return total;
    }

    @Override
    public String toString() {
      return "PageViews{" +
              "counts=" + counts +
              ", from=" + FORMAT.format(new Date(from)) +
              ", to=" + FORMAT.format(new Date(to)) +
              '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PageViews pageViews1 = (PageViews) o;

      if (from != null ? !from.equals(pageViews1.from) : pageViews1.from != null) return false;
      if (counts != null ? !counts.equals(pageViews1.counts) : pageViews1.counts != null) return false;
      if (to != null ? !to.equals(pageViews1.to) : pageViews1.to != null) return false;

      return true;
    }

    public static class Count {
      String url; long increment = 1L;
      private Count() {

      }
      public Count(String url, long increment) {
        this.url = url;
        this.increment = increment;
      }

      public String getUrl() {
        return url;
      }

      public long getIncrement() {
        return increment;
      }

      public void increment(long value) {
        this.increment += value;
      }
    }

    @Override
    public int hashCode() {
      int result = from != null ? from.hashCode() : 0;
      result = 31 * result + (to != null ? to.hashCode() : 0);
      return result;
    }
  }
}
