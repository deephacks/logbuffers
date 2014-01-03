package org.deephacks.logbuffers;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A description of how the tail should behave in terms of scheduling.
 * This basic schedule forwards the log processing periodically by notifying
 * the tail with all unprocessed logs each round.
 */
public class TailSchedule {
  private final int delay;
  private final TimeUnit unit;

  private long backLogScheduleDelay;
  private TimeUnit backLogScheduleUnit;

  private final Tail<?> tail;

  private TailSchedule(Builder<?> builder) {
    this.delay = Optional.fromNullable(builder.delay).or(15);
    this.unit = Optional.fromNullable(builder.unit).or(SECONDS);
    this.tail = Preconditions.checkNotNull(builder.tail);
    this.backLogScheduleDelay = builder.backLogScheduleDelay;
    this.backLogScheduleUnit = Optional.fromNullable(builder.backLogScheduleUnit).or(TimeUnit.MILLISECONDS);
  }

  public Tail<?> getTail() {
    return tail;
  }

  public int getDelay() {
    return delay;
  }

  public TimeUnit getUnit() {
    return unit;
  }

  public long getBackLogScheduleDelay() {
    return backLogScheduleDelay;
  }

  public TimeUnit getBackLogScheduleUnit() {
    return backLogScheduleUnit;
  }

  public static abstract class Builder<T extends Builder<T>> {
    private Integer delay;
    private TimeUnit unit;

    private long backLogScheduleDelay;
    private TimeUnit backLogScheduleUnit;

    private Tail<?> tail;

    protected Builder(Tail<?> tail) {
      this.tail = tail;
    }

    protected abstract T self();

    /**
     * Set the delay between each round. Default is 15 seconds.
     */
    public T delay(Integer delay, TimeUnit unit) {
      this.delay = delay;
      this.unit = unit;
      return self();
    }

    /**
     * How long to wait before processing the backlog. Default is 0 seconds.
     */
    public T backLogSchedule(long delay, TimeUnit unit) {
      this.backLogScheduleDelay = delay;
      this.backLogScheduleUnit = unit;
      return self();
    }

    public TailSchedule build() {
      return new TailSchedule(this);
    }
  }

  private static class DefaultBuilder extends Builder<DefaultBuilder> {

    private DefaultBuilder(Tail<?> tail) {
      super(tail);
    }

    @Override
    protected DefaultBuilder self() {
      return this;
    }
  }

  /**
   * @return the tail of this schedule
   */
  public static Builder<?> builder(Tail<?> tail) {
    return new DefaultBuilder(tail);
  }

  /**
   * Same as the default except that logs are sliced iteratively into chunks according to a certain
   * period of time until all unprocessed logs are finished.
   */
  public static class TailScheduleChunk extends TailSchedule {
    private final long chunkMs;

    protected TailScheduleChunk(Builder<?> builder) {
      super(builder);
      this.chunkMs = Optional.fromNullable(builder.chunkMs).or(TimeUnit.SECONDS.toMillis(15));
    }

    public long getChunkMs() {
      return chunkMs;
    }

    public static abstract class Builder<T extends Builder<T>> extends TailSchedule.Builder<T> {
      private long chunkMs;

      protected Builder(Tail<?> tail) {
        super(tail);
      }

      /**
       * Set the length of each chunk. Default is 15 seconds.
       */
      public T chunkLength(long duration, TimeUnit unit) {
        this.chunkMs = unit.toMillis(duration);
        return self();
      }

      public TailScheduleChunk build() {
        return new TailScheduleChunk(this);
      }
    }

    private static class DefaultBuilder extends Builder<DefaultBuilder> {

      protected DefaultBuilder(Tail<?> tail) {
        super(tail);
      }

      @Override
      protected DefaultBuilder self() {
        return this;
      }
    }

    public static Builder<?> builder(Tail<?> tail) {
      return new DefaultBuilder(tail);
    }
  }
}
