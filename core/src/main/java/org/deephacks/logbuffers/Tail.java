package org.deephacks.logbuffers;

/**
 * A tail tracks a specific data type attached to a specific log buffer. A log buffer
 * can have many different tail classes, tracking same or different log types.
 * Each tail class have a separate index tracker that does not affect other indexes.
 *
 * Note that there can only be one instance per tail class! If more tails
 * are needed, several classes must be defined and managed separately.
 *
 * @param <T> log type of interest.
 */
public interface Tail<T> {
  /**
   * Process a set of logs. This is an all or nothing operation!
   *
   * Logs will be retried forever until this method returns successfully
   * at which point the logs are considered processed and the index will advance.
   *
   * @param logs to be processed ordered sequentially according to index and timestamp.
   * @throws RuntimeException processing failure, logs are retried next round.
   */
  public void process(Logs<T> logs) throws RuntimeException;
}
