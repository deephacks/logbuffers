package org.deephacks.logbuffers;

import java.util.List;

/**
 * A tail tracks a specific data type attached to a specific log buffer. A log buffer
 * can have many tails. Each tail have separate a index tracker that does not affect
 * other indexes.
 *
 * @param <T> log type of interest.
 */
public interface Tail<T> {
  /**
   * Process a set of logs. Logs are considered processed after this
   * method returns without throwing an exception.
   *
   * Log processing will retry indefinitely until does not throw a runtime
   * exception anymore, at which point the logs are considered processed and
   * the index will advance.
   *
   * @param logs to be processed.
   */
  public void process(List<T> logs);
}
