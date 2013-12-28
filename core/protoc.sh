protoc src/test/resources/descriptor.proto --java_out=src/test/java
protoc --descriptor_set_out=src/test/resources/descriptor.desc src/test/resources/descriptor.proto

protoc src/test/resources/log.proto --java_out=src/test/java
protoc --descriptor_set_out=src/test/resources/log.desc src/test/resources/log.proto
