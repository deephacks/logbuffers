package org.deephacks.logbuffers;

/**
 * Container for the object and the log.
 *
 * @param <T>
 */
public final class Log<T> {
  private final RawLog rawLog;
  private final T object;

  Log(RawLog rawLog, T object) {
    this.rawLog = rawLog;
    this.object = object;
  }

  /**
   * @return the log
   */
  public RawLog getLog() {
    return rawLog;
  }

  /**
   * @return the object
   */
  public T get() {
    return object;
  }
}
