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

import java.util.LinkedList;

/**
 * A list of logs.
 */
public final class Logs<T> {

  /** sequentially ordered logs according to index and timestamp */
  private LinkedList<Log<T>> logs = new LinkedList<>();

  /**
   * the buffer is responsible for putting the logs in the correct order
   */
  void put(T object, LogRaw log) {
    logs.add(new Log<>(log, object));
  }

  /**
   * Get all logs ordered sequentially according to index and timestamp
   *
   * @return real object and log meta data.
   */
  public LinkedList<Log<T>> getLogs() {
    return logs;
  }

  /**
   * Get all objects ordered sequentially according to index and timestamp
   *
   * @return the real objects that represent the logs
   */
  public LinkedList<T> get() {
    LinkedList<T> objects = new LinkedList<>();
    for (Log<T> log : logs) {
      objects.add(log.get());
    }
    return objects;
  }

  /**
   * @return the first object
   */
  public T getFirst() {
    return logs.getFirst().get();
  }

  /**
   * @return the first log
   */
  public LogRaw getFirstLog() {
    return logs.getFirst().getLog();
  }

  /**
   * @return the last object
   */
  public T getLast() {
    return logs.getLast().get();
  }

  /**
   * @return the last log
   */
  public LogRaw getLastLog() {
    return logs.getLast().getLog();
  }

  /**
   * @return number of logs
   */
  public int size() {
    return logs.size();
  }

  /**
   * @return if logs are empty
   */
  public boolean isEmpty() {
    return logs.isEmpty();
  }
}
