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
   * @param object to fetch the raw log for
   * @return log the contain meta data such as index, timestamp etc.
   */
  public Log getLog(T object) {
    return logs.get(object);
  }

  /**
   * @return the last log contained in this list of logs
   */
  public Log getLastLog() {
    T last = getObjects().getLast();
    return logs.get(last);
  }

  /**
   * @return number of logs
   */
  public int size() {
    return logs.size();
  }
}
