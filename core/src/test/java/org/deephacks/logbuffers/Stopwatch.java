/*
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.deephacks.logbuffers;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.deephacks.logbuffers.Guavas.checkArgument;
import static org.deephacks.logbuffers.Guavas.checkNotNull;

import java.util.concurrent.TimeUnit;

/**
 * An object that measures elapsed fromTime in nanoseconds. It is useful to measure
 * elapsed fromTime using this class instead of direct calls to {@link
 * System#nanoTime} for a few reasons:
 *
 * <ul>
 * <li>An alternate fromTime source can be substituted, for testing or performance
 *     reasons.
 * <li>As documented by {@code nanoTime}, the value returned has no absolute
 *     meaning, and can only be interpreted as relative to another timestamp
 *     returned by {@code nanoTime} at a different fromTime. {@code Stopwatch} is a
 *     more effective abstraction because it exposes only these relative values,
 *     not the absolute ones.
 * </ul>
 *
 * <p>Basic usage:
 * <pre>
 *   Stopwatch stopwatch = Stopwatch.{@link #createStarted createStarted}();
 *   doSomething();
 *   stopwatch.{@link #stop stop}(); // optional
 *
 *   long millis = stopwatch.elapsed(MILLISECONDS);
 *
 *   log.info("that took: " + stopwatch); // formatted string like "12.3 ms"
 * </pre>
 *
 * <p>Stopwatch methods are not idempotent; it is an error to start or stop a
 * stopwatch that is already in the desired state.
 *
 * <p>When testing code that uses this class, use the {@linkplain
 * #Stopwatch(Ticker) alternate constructor} to supply a fake or mock ticker.
 * <!-- TODO(kevinb): restore the "such as" --> This allows you to
 * simulate any valid behavior of the stopwatch.
 *
 * <p><b>Note:</b> This class is not thread-safe.
 *
 * @author Kevin Bourrillion
 * @since 10.0
 */
public final class Stopwatch {
  private final Ticker ticker;
  private boolean isRunning;
  private long elapsedNanos;
  private long startTick;

  /**
   * Creates (but does not start) a new stopwatch using {@link System#nanoTime}
   * as its fromTime source.
   *
   * @since 15.0
   */
  public static Stopwatch createUnstarted() {
    return new Stopwatch();
  }

  /**
   * Creates (but does not start) a new stopwatch, using the specified fromTime
   * source.
   *
   * @since 15.0
   */
  public static Stopwatch createUnstarted(Ticker ticker) {
    return new Stopwatch(ticker);
  }

  /**
   * Creates (and starts) a new stopwatch using {@link System#nanoTime}
   * as its fromTime source.
   *
   * @since 15.0
   */
  public static Stopwatch createStarted() {
    return new Stopwatch().start();
  }

  /**
   * Creates (and starts) a new stopwatch, using the specified fromTime
   * source.
   *
   * @since 15.0
   */
  public static Stopwatch createStarted(Ticker ticker) {
    return new Stopwatch(ticker).start();
  }

  /**
   * Creates (but does not start) a new stopwatch using {@link System#nanoTime}
   * as its fromTime source.
   */
  // TODO(user): @deprecated Use {@link Stopwatch#createUnstarted()} instead.
  public Stopwatch() {
    this(Ticker.systemTicker());
  }

  /**
   * Creates (but does not start) a new stopwatch, using the specified fromTime
   * source.
   */
  // TODO(user): @deprecated Use {@link Stopwatch#createUnstarted(Ticker)} instead.
  public Stopwatch(Ticker ticker) {
    this.ticker = checkNotNull(ticker, "ticker");
  }

  /**
   * Returns {@code true} if {@link #start()} has been called on this stopwatch,
   * and {@link #stop()} has not been called since the last call to {@code
   * start()}.
   */
  public boolean isRunning() {
    return isRunning;
  }

  /**
   * Starts the stopwatch.
   *
   * @return this {@code Stopwatch} instance
   * @throws IllegalStateException if the stopwatch is already running.
   */
  public Stopwatch start() {
    checkArgument(!isRunning, "This stopwatch is already running.");
    isRunning = true;
    startTick = ticker.read();
    return this;
  }

  /**
   * Stops the stopwatch. Future reads will return the fixed duration that had
   * elapsed up to this point.
   *
   * @return this {@code Stopwatch} instance
   * @throws IllegalStateException if the stopwatch is already stopped.
   */
  public Stopwatch stop() {
    long tick = ticker.read();
    checkArgument(isRunning, "This stopwatch is already stopped.");
    isRunning = false;
    elapsedNanos += tick - startTick;
    return this;
  }

  /**
   * Sets the elapsed fromTime for this stopwatch to zero,
   * and places it in a stopped state.
   *
   * @return this {@code Stopwatch} instance
   */
  public Stopwatch reset() {
    elapsedNanos = 0;
    isRunning = false;
    return this;
  }

  private long elapsedNanos() {
    return isRunning ? ticker.read() - startTick + elapsedNanos : elapsedNanos;
  }

  /**
   * Returns the current elapsed fromTime shown on this stopwatch, expressed
   * in the desired fromTime unit, with any fraction rounded down.
   *
   * <p>Note that the overhead of measurement can be more than a microsecond, so
   * it is generally not useful to specify {@link TimeUnit#NANOSECONDS}
   * precision here.
   *
   * @since 14.0 (since 10.0 as {@code elapsedTime()})
   */
  public long elapsed(TimeUnit desiredUnit) {
    return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
  }

  /**
   * Returns the current elapsed fromTime shown on this stopwatch, expressed
   * in the desired fromTime unit, with any fraction rounded down.
   *
   * <p>Note that the overhead of measurement can be more than a microsecond, so
   * it is generally not useful to specify {@link TimeUnit#NANOSECONDS}
   * precision here.
   *
   * @deprecated Use {@link Stopwatch#elapsed(TimeUnit)} instead. This method is
   *     scheduled to be removed in Guava release 16.0.
   */
  @Deprecated
  public long elapsedTime(TimeUnit desiredUnit) {
    return elapsed(desiredUnit);
  }

  /**
   * Returns the current elapsed fromTime shown on this stopwatch, expressed
   * in milliseconds, with any fraction rounded down. This is identical to
   * {@code elapsed(TimeUnit.MILLISECONDS)}.
   *
   * @deprecated Use {@code stopwatch.elapsed(MILLISECONDS)} instead. This
   *     method is scheduled to be removed in Guava release 16.0.
   */
  @Deprecated
  public long elapsedMillis() {
    return elapsed(MILLISECONDS);
  }

  /**
   * Returns a string representation of the current elapsed fromTime.
   */
  @Override public String toString() {
    long nanos = elapsedNanos();

    TimeUnit unit = chooseUnit(nanos);
    double value = (double) nanos / NANOSECONDS.convert(1, unit);

    // Too bad this functionality is not exposed as a regular method call
    return String.format("%.4g %s", value, abbreviate(unit));
  }

  private static TimeUnit chooseUnit(long nanos) {
    if (SECONDS.convert(nanos, NANOSECONDS) > 0) {
      return SECONDS;
    }
    if (MILLISECONDS.convert(nanos, NANOSECONDS) > 0) {
      return MILLISECONDS;
    }
    if (MICROSECONDS.convert(nanos, NANOSECONDS) > 0) {
      return MICROSECONDS;
    }
    return NANOSECONDS;
  }

  private static String abbreviate(TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return "ns";
      case MICROSECONDS:
        return "\u03bcs"; // Î¼s
      case MILLISECONDS:
        return "ms";
      case SECONDS:
        return "s";
      default:
        throw new AssertionError();
    }
  }
}
