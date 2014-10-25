package org.deephacks.logbuffers.avro;


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.Message;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.deephacks.logbuffers.LogSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroSerializer implements LogSerializer {

  private BiMap<Long, Class<?>> mapping = HashBiMap.create();
  private Map<Long, SpecificDatumReader<Object>> schemaCache = new HashMap<>();

  public AvroSerializer() {
    mapping.put(120L, PageView.class);
    schemaCache.put(120L, new SpecificDatumReader<>(PageView.SCHEMA$));
    mapping.put(121L, Visit.class);
    schemaCache.put(121L, new SpecificDatumReader<>(Visit.SCHEMA$));
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
  public byte[] serialize(Object msg) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    Schema schema = getClassSchema(msg.getClass());
    DatumWriter<Object> writer = new SpecificDatumWriter<>(schema);
    try {
      writer.write(msg, encoder);
      encoder.flush();
      out.close();
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object deserialize(byte[] log, long type) {
    try {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(log, null);
      return schemaCache.get(type).read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final ConcurrentHashMap<Class<?>, Schema> schemas = new ConcurrentHashMap<>();

  public static <T> Schema getClassSchema(Class<T> cls) {
    Schema schema = schemas.get(cls);
    if (schema == null) {
      try {
        schema = (Schema) cls.getMethod("getClassSchema").invoke(null);
        schemas.putIfAbsent(cls, schema);
        return schemas.get(cls);
      } catch (Exception  e) {
        throw new RuntimeException(e);
      }
    }
    return schema;
  }
}
