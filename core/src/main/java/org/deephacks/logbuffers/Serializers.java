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

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.HashMap;
import java.util.Map;

class Serializers {
  private Map<Long, ObjectLogSerializer> typeToSerializer = new HashMap<>();
  private Map<Class<?>, ObjectLogSerializer> classToSerializer = new HashMap<>();
  private BiMap<Long, Class<?>> mapping = HashBiMap.create();

  void addSerializer(ObjectLogSerializer serializer) {
    BiMap<Long, Class<?>> mapping = serializer.getMapping();
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
      this.mapping.put(type, cls);
    }
  }

  ObjectLogSerializer getSerializer(Long type) {
    return typeToSerializer.get(type);
  }

  ObjectLogSerializer getSerializer(Class<?> cls) {
    return classToSerializer.get(cls);
  }

  Long getType(Class<?> cls) {
    return Preconditions.checkNotNull(mapping.inverse().get(cls), "No mapping found for " + cls);
  }
}
