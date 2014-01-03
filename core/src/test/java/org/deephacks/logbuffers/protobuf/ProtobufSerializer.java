package org.deephacks.logbuffers.protobuf;


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.Message;
import org.deephacks.logbuffers.ObjectLogSerializer;
import org.deephacks.logbuffers.protobuf.ProtoLog.PageView;
import org.deephacks.logbuffers.protobuf.ProtoLog.Visit;

import java.lang.reflect.Method;

public class ProtobufSerializer implements ObjectLogSerializer {

  private BiMap<Long, Class<?>> mapping = HashBiMap.create();

  public ProtobufSerializer() {
    mapping.put(120L, PageView.class);
    mapping.put(121L, Visit.class);
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
      Method parseFrom = mapping.get(type).getMethod("parseFrom", byte[].class);
      return parseFrom.invoke(null, log);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
