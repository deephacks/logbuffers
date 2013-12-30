package org.deephacks.logbuffers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Logs<T> {
  private LinkedHashMap<T, Log> logs = new LinkedHashMap<>();
  private ArrayList<T> objects;

  void put(T object, Log log) {
    logs.put(object, log);
  }

  public Log get(T object) {
    return logs.get(object);
  }

  public List<T> get() {
    if (objects == null) {
      objects = new ArrayList<>(logs.keySet());
    }
    return objects;
  }

  public int size() {
    return logs.size();
  }

  public Log getLastLog() {
    T last = get().get(objects.size() - 1);
    return logs.get(last);
  }
}
