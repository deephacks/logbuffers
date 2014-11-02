package org.deephacks.logbuffers.protobuf;


import com.google.protobuf.Message;
import org.deephacks.logbuffers.LogSerializer;
import org.deephacks.logbuffers.protobuf.ProtoLog.PageView;
import org.deephacks.logbuffers.protobuf.ProtoLog.Visit;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ProtobufSerializer implements LogSerializer {

  private HashMap<Long, Class<?>> mapping = new HashMap<>();
  private Map<Long, Method> methodCache = new HashMap<>();

  public ProtobufSerializer() {
    mapping.put(120L, PageView.class);
    methodCache.put(120L, getParseMethod(PageView.class));
    mapping.put(121L, Visit.class);
    methodCache.put(121L, getParseMethod(Visit.class));
  }

  private Method getParseMethod(Class<?> cls) {
    try {
      return cls.getMethod("parseFrom", byte[].class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public HashMap<Long, Class<?>> getMappingForward() {
    return mapping;
  }

  @Override
  public byte[] serialize(Object proto) {
    Message msg = (Message) proto;
    return msg.toByteArray();
  }

  @Override
  public Object deserialize(byte[] log, long type) {
    try {
      return methodCache.get(type).invoke(null, log);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
