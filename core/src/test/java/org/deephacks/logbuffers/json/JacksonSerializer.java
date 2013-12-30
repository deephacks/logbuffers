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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JacksonSerializer implements ObjectLogSerializer {

  private BiMap<Long, Class<?>> mapping = HashBiMap.create();

  private ObjectMapper mapper = new ObjectMapper();

  public JacksonSerializer() {
    mapping.put(123L, A.class);
    mapping.put(124L, B.class);
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

}
