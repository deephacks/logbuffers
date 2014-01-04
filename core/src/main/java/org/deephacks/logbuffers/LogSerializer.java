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

import com.google.common.collect.BiMap;

/**
 * Serialize objects to and from raw object log format.
 *
 * Each serializer needs to keep a mapping between class and a unique identifier.
 *
 * The reason for this mapping is to save computing resources. Consider a
 * regular java class name that occupy around 50 bytes and a system that produce
 * 10000 logs/sec. This is roughly 0.5MB per sec (or 1.8 GB per day) of wasted
 * latency, space and processing power.
 */
public interface LogSerializer {

  /**
   * Get the unique number mapped to a specific class.
   *
   * @return unique number or absent if no mapping exist.
   */
  public BiMap<Long, Class<?>> getMapping();

  /**
   * Called when a new object is written to the object log buffer that
   * this json is attached to.
   *
   * @param object that is written.
   * @return a raw object log.
   */
  public byte[] serialize(Object object);

  /**
   * Called when the object log buffer is queried with an index range that covers a specific log.
   *
   * Note that logs not understood by this specific json might also be provided, hence the
   * Optional return object.
   *
   * @param log that was queried.
   * @param type the unique number that map the binary data to a specific class.
   * @return Present if this json know how to serialize the log object.
   */
  public Object deserialize(byte[] log, long type);

}
