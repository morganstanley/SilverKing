package com.ms.silverking.time;

import static com.ms.silverking.time.TimeUtils.*;

import java.math.BigDecimal;

/**
 * <p>Provides the core of a basic Stopwatch implementation. State stored by this
 * abstract class is restricted to start and stop times to keep memory
 * utilization to a minimum. Concrete classes can decide how much additional
 * state is desired.</p>
 *
 * <p><b>NOTE: sanity checking in this class is minimal to keep this class lean;
 * concrete classes can add more if desired.</b></p>
 *
 * <p>This class is <b>not</b> completely threadsafe though
 * portions may safely be used by multiple threads.</p>
 */
public abstract class StopwatchBase implements Stopwatch {
  private long startTimeNanos;
  private long stopTimeNanos;

  protected StopwatchBase(long startTimeNanos) {
    this.startTimeNanos = startTimeNanos;
  }

  protected abstract long relTimeNanos();

  protected void ensureState(State requiredState) {
    if (getState() != requiredState) {
      throw new RuntimeException("Stopwatch state not: " + requiredState);
    }
  }

  // control

  @Override
  public void start() {
    // (See sanity checking note above)
    stopTimeNanos = 0;
    startTimeNanos = relTimeNanos();
  }

  @Override
  public void stop() {
    // (See sanity checking note above)
    stopTimeNanos = relTimeNanos();
  }

  @Override
  public void reset() {
    stopTimeNanos = 0;
    startTimeNanos = relTimeNanos();
  }

  // elapsed

  @Override
  public long getElapsedNanos() {
    // (See sanity checking note above)
    return stopTimeNanos - startTimeNanos;
  }

  @Override
  public long getElapsedMillisLong() {
    return nanos2millisLong(getElapsedNanos());
  }

  @Override
  public int getElapsedMillis() {
    long elapsedMillis;

    elapsedMillis = getElapsedMillisLong();
    checkTooManyMillis(elapsedMillis);

    return (int) elapsedMillis;
  }

  @Override
  public double getElapsedSeconds() {
    return nanos2seconds(getElapsedNanos());
  }

  @Override
  public BigDecimal getElapsedSecondsBD() {
    return nanos2secondsBD(getElapsedNanos());
  }

  // split

  @Override
  public long getSplitNanos() {
    long curTimeNanos;

    // (See sanity checking note above)
    curTimeNanos = relTimeNanos();
    if (isStopped()) {
      curTimeNanos = stopTimeNanos;
    }
    return curTimeNanos - startTimeNanos;
  }

  @Override
  public long getSplitMillisLong() {
    return nanos2millisLong(getSplitNanos());
  }

  @Override
  public int getSplitMillis() {
    long splitMillis;

    splitMillis = getSplitMillisLong();
    checkTooManyMillis(splitMillis);

    return (int) splitMillis;
  }

  @Override
  public double getSplitSeconds() {
    return nanos2seconds(getSplitNanos());
  }

  @Override
  public BigDecimal getSplitSecondsBD() {
    return nanos2secondsBD(getSplitNanos());
  }

  // misc.

  @Override
  public String getName() {
    return "";
  }

  @Override
  public State getState() {
    if (isRunning()) {
      return State.running;
    } else {
      return State.stopped;
    }
  }

  @Override
  public boolean isRunning() {
    return stopTimeNanos == 0;
  }

  @Override
  public boolean isStopped() {
    return !isRunning();
  }

  public String toStringElapsed() {
    return getName() + ":" + getState() + ":" + getElapsedSeconds();
  }

  public String toStringSplit() {
    return getName() + ":" + getState() + ":" + getSplitSeconds();
  }

  @Override
  public String toString() {
    return toStringSplit();
  }
}
