package org.deephacks.logbuffers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Logs<T> {
  private LinkedHashMap<T, Log> logs = new LinkedHashMap<>();

  void put(T object, Log log) {
    logs.put(object, log);
  }

  public Log get(T object) {
    return logs.get(object);
  }

  public List<T> get() {
    return new ArrayList<>(logs.keySet());
  }

  public int size() {
    return logs.size();
  }
}
