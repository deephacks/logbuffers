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
    public Log serialize(Object object) {
        Long type = classTypeMapping.inverse().get(object.getClass());
        Preconditions.checkNotNull(type, "No type mapped to class " + object.getClass());
        StringWriter writer = new StringWriter();
        try {
            mapper.writeValue(writer, object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] bytes = writer.toString().getBytes(Charsets.UTF_8);
        return new Log(type, bytes);
    }

    @Override
    public <T> Optional<T> deserialize(Log log, Class<T> type) {
        Class<T> cls = (Class<T>) classTypeMapping.get(log.getType());
        if (cls == null) {
            return Optional.absent();
        }
        try {
            return Optional.fromNullable(mapper.readValue(log.getContent(), cls));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
