package org.deephacks.logbuffers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.IOException;
import java.io.StringWriter;

public class JacksonSerializer implements ObjectLogSerializer {
    private BiMap<Long, Class<?>> classTypeMapping = HashBiMap.create();

    private ObjectMapper mapper = new ObjectMapper();

    public void put(Class<?> cls, Long type) {
        classTypeMapping.put(type, cls);
    }

    @Override
    public Optional<Long> getType(Class<?> type) {
        return Optional.fromNullable(classTypeMapping.inverse().get(type));
    }

    @Override
    public byte[] serialize(Object object) {
        Long type = classTypeMapping.inverse().get(object.getClass());
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
    public <T> Optional<T> deserialize(byte[] log, long type) {
        Class<T> cls = (Class<T>) classTypeMapping.get(type);
        if (cls == null) {
            return Optional.absent();
        }
        try {
            return Optional.fromNullable(mapper.readValue(log, cls));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
