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

import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * A list of logs.
 */
public class Logs<T> {

  /** sequentially ordered logs according to index and timestamp */
  private LinkedHashMap<T, Log> logs = new LinkedHashMap<>();

  /** the actual objects */
  private LinkedList<T> objects;

  /**
   * the buffer is responsible for putting the logs in the correct order
   */
  void put(T object, Log log) {
    logs.put(object, log);
  }

  /**
   * Get all logs ordered sequentially according to index and timestamp
   *
   * @return the real objects that represent the logs
   */
  public LinkedList<T> getObjects() {
    if (objects == null) {
      objects = new LinkedList<>(logs.keySet());
    }
    return objects;
  }

  /**
   * @return the first object
   */
  public T getFirst() {
    return getObjects().getFirst();
  }

  public Log getFirstLog() {
    return getLog(getObjects().getFirst());
  }


  /**
   * @return the last object
   */
  public T getLast() {
    return getObjects().getLast();
  }

  /**
   * @return the last log contained in this list of logs
   */
  public Log getLastLog() {
    T last = getObjects().getLast();
    return logs.get(last);
  }


  /**
   * @param object to fetch the raw log for
   * @return log the contain meta data such as index, timestamp etc.
   */
  public Log getLog(T object) {
    return logs.get(object);
  }


  /**
   * @return number of logs
   */
  public int size() {
    return logs.size();
  }

  public boolean isEmpty() {
    return logs.isEmpty();
  }
}
