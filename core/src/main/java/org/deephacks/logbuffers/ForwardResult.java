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


import com.google.common.base.Optional;

import java.util.concurrent.TimeUnit;

class ForwardResult {

  private Optional<ScheduleAgain> scheduleAgain = Optional.absent();

  ForwardResult() {

  }

  ForwardResult(ScheduleAgain scheduleAgain) {
    this.scheduleAgain = Optional.fromNullable(scheduleAgain);
  }

  public static ForwardResult scheduleAgain(long delay, TimeUnit unit) {
    return new ForwardResult(new ScheduleAgain(delay, unit));
  }

  Optional<ScheduleAgain> scheduleAgain() {
    return scheduleAgain;
  }

  public static final class ScheduleAgain {
    private long delay;
    private TimeUnit timeUnit;

    private ScheduleAgain(long delay, TimeUnit timeUnit) {
      this.delay = delay;
      this.timeUnit = timeUnit;
    }

    public long getDelay() {
      return delay;
    }

    public TimeUnit getTimeUnit() {
      return timeUnit;
    }
  }
}
