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
