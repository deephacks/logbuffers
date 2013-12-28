package org.deephacks.logbuffers.protobuf;


import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import org.deephacks.logbuffers.ObjectLogSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class ProtobufSerializer implements ObjectLogSerializer {
    private HashMap<Integer, Method> numToMethod = new HashMap<>();
    private HashMap<String, Integer> protoToNum = new HashMap<>();
    private HashMap<Class<?>, Long> clsToNum = new HashMap<>();

    public ProtobufSerializer() {
        registerResource("log.desc");
    }

    public void register(URL protodesc) {
        try {
            registerDesc(protodesc.getFile(), protodesc.openStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void register(File protodesc) {
        try {
            registerDesc(protodesc.getName(), new FileInputStream(protodesc));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerResource(String protodesc) {
        URL url = Thread.currentThread().getContextClassLoader().getResource(protodesc);
        register(url);
    }

    private void registerDesc(String name, InputStream in) {
        try {
            FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(in);
            for (FileDescriptorProto fdp : descriptorSet.getFileList()) {
                FileDescriptor fd = FileDescriptor.buildFrom(fdp, new FileDescriptor[]{});

                for (Descriptor desc : fd.getMessageTypes()) {
                    FieldDescriptor fdesc = desc.findFieldByName("protoType");
                    if (fdesc == null) {
                        throw new IllegalArgumentException(name
                                + ".proto file must define protoType field "
                                + "with unqiue number that identify proto type");
                    }
                    String packageName = fdp.getOptions().getJavaPackage();

                    if (Strings.isNullOrEmpty(packageName)) {
                        throw new IllegalArgumentException(name
                                + ".proto file must define java_package");
                    }
                    String simpleClassName = fdp.getOptions().getJavaOuterClassname();
                    if (Strings.isNullOrEmpty(simpleClassName)) {
                        throw new IllegalArgumentException(name
                                + " .proto file must define java_outer_classname");
                    }

                    String className = packageName + "." + simpleClassName + "$" + desc.getName();
                    Class<?> cls = Thread.currentThread().getContextClassLoader()
                            .loadClass(className);
                    clsToNum.put(cls, (long) fdesc.getNumber());
                    protoToNum.put(desc.getFullName(), fdesc.getNumber());
                    numToMethod.put(fdesc.getNumber(), cls.getMethod("parseFrom", byte[].class));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Long> getType(Class<?> type) {
        return Optional.fromNullable(clsToNum.get(type));
    }

    @Override
    public byte[] serialize(Object proto) {
        Message msg = (Message) proto;
        String protoName = msg.getDescriptorForType().getFullName();
        Integer num = protoToNum.get(protoName);
        if(num == null){
            throw new IllegalArgumentException("Could not find protoType mapping for " + protoName);
        }
        byte[] msgBytes = msg.toByteArray();
        Varint32 vint = new Varint32(num);
        int vsize = vint.getSize();
        byte[] bytes = new byte[vsize + msgBytes.length];
        try {
            System.arraycopy(vint.write(), 0, bytes, 0, vsize);
            System.arraycopy(msgBytes, 0, bytes, vsize, msgBytes.length);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> Optional<T> deserialize(byte[] log, long type) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(log);
            Varint32 vint = new Varint32(buf);
            int protoTypeNum = vint.read();
            buf = vint.getByteBuffer();
            byte[] message = new byte[buf.remaining()];
            buf.get(message);
            Method m = numToMethod.get(protoTypeNum);
            if (m == null) {
                throw new IllegalArgumentException("No method found");
            }
            return (Optional<T>) Optional.fromNullable(m.invoke(null, message));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
