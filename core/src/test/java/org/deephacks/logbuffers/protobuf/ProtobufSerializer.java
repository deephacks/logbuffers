package org.deephacks.logbuffers.protobuf;


import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.Message;
import org.deephacks.logbuffers.ObjectLogSerializer;
import org.deephacks.logbuffers.protobuf.ProtoLog.PageView;
import org.deephacks.logbuffers.protobuf.ProtoLog.Visit;

import java.lang.reflect.Method;

public class ProtobufSerializer implements ObjectLogSerializer {
    private BiMap<Long, Class<?>> classTypeMapping = HashBiMap.create();

    public ProtobufSerializer() {
        classTypeMapping.put(123L, PageView.class);
        classTypeMapping.put(124L, Visit.class);
    }

    @Override
    public Optional<Long> getType(Class<?> type) {
        return Optional.fromNullable(classTypeMapping.inverse().get(type));
    }

    @Override
    public byte[] serialize(Object proto) {
        Message msg = (Message) proto;
        return msg.toByteArray();
    }

    @Override
    public <T> Optional<T> deserialize(byte[] log, long type) {
        try {
            Method parseFrom = classTypeMapping.get(type).getMethod("parseFrom", byte[].class);
            return (Optional<T>) Optional.fromNullable(parseFrom.invoke(null, log));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
