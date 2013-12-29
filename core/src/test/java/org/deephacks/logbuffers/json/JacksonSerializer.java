package org.deephacks.logbuffers.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.deephacks.logbuffers.ObjectLogSerializer;
import org.deephacks.logbuffers.LogUtil.A;
import org.deephacks.logbuffers.LogUtil.B;

import java.io.IOException;
import java.io.StringWriter;

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
}
