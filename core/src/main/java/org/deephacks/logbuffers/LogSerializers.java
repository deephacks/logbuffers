/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.logbuffers;

import java.util.HashMap;
import java.util.Map;

import static org.deephacks.logbuffers.Guavas.checkNotNull;

class LogSerializers {
  private Map<Long, LogSerializer> typeToSerializer = new HashMap<>();
  private Map<Class<?>, LogSerializer> classToSerializer = new HashMap<>();
  private HashMap<Long, Class<?>> mappingForward = new HashMap<>();
  private HashMap<Class<?>, Long> mappingBackward = new HashMap<>();

  void addSerializer(LogSerializer serializer) {
    HashMap<Long, Class<?>> mapping = serializer.getMappingForward();
    for (Long type : mapping.keySet()) {
      if (typeToSerializer.containsKey(type)) {
        throw new IllegalArgumentException("Overlapping serializers for type " + type);
      }
      typeToSerializer.put(type, serializer);
      Class<?> cls = mapping.get(type);
      if (classToSerializer.containsKey(cls)) {
        throw new IllegalArgumentException("Overlapping serializers for class " + cls);
      }
      classToSerializer.put(cls, serializer);
      this.mappingForward.put(type, cls);
      this.mappingBackward.put(cls, type);
    }
  }

  LogSerializer getSerializer(Long type) {
    return typeToSerializer.get(type);
  }

  LogSerializer getSerializer(Class<?> cls) {
    return classToSerializer.get(cls);
  }

  Long getType(Class<?> cls) {
    return checkNotNull(mappingBackward.get(cls), "No mapping found for " + cls);
  }
}
