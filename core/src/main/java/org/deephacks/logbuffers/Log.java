package org.deephacks.logbuffers;

/**
 * Container for the object and the log.
 *
 * @param <T>
 */
public final class Log<T> {
  private final LogRaw rawLog;
  private final T object;

  Log(LogRaw rawLog, T object) {
    this.rawLog = rawLog;
    this.object = object;
  }

  /**
   * @return the log
   */
  public LogRaw getLog() {
    return rawLog;
  }

  /**
   * @return the object
   */
  public T get() {
    return object;
  }
}
