package org.deephacks.logbuffers;

public class Log<T> {
  private RawLog rawLog;
  private T object;

  public Log(RawLog rawLog, T object) {
    this.rawLog = rawLog;
    this.object = object;
  }

  public RawLog getRaw() {
    return rawLog;
  }

  public T get() {
    return object;
  }
}
