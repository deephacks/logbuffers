package org.deephacks.logbuffers.protobuf;


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.Message;
import org.deephacks.logbuffers.ObjectLogSerializer;
import org.deephacks.logbuffers.protobuf.ProtoLog.PageView;
import org.deephacks.logbuffers.protobuf.ProtoLog.Visit;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ProtobufSerializer implements ObjectLogSerializer {

  private BiMap<Long, Class<?>> mapping = HashBiMap.create();
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
  public BiMap<Long, Class<?>> getMapping() {
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
