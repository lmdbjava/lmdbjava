package org.lmdbjava;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class StripedRefCounter implements RefCounter {
  private static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();
  /**
   * Counter value used to indicate a count of zero while a sum of all counters is being
   * performed.
   */
  private static final int MAGIC_ZERO_VALUE = Integer.MIN_VALUE;
  /**
   * Counter value used to indicate that this RefCounter has been closed.
   */
  private static final int MAGIC_CLOSED_VALUE = Integer.MAX_VALUE;
  /**
   * The maximum possible count value on one stripe.
   */
  private static final int MAX_COUNTER_VALUE = Integer.MAX_VALUE - 1;
  private static final int DEFAULT_STRIPES = 64;
  /**
   * Maximum number of stripes.
   */
  private static final int MAX_STRIPES = 256;

  private final AtomicInteger[] counters;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  /**
   * Bit mask for fast stripe index calculation. Equal to (stripeCount - 1).
   * Used with bitwise AND for O(1) hashing with no modulo operation.
   */
  private final int stripeMask;

  StripedRefCounter() {
    this(getDefaultStripeCount());
  }

  StripedRefCounter(final int stripeCount) {
    validateStripeCount(stripeCount);
    this.stripeMask = stripeCount - 1;
    this.counters = new AtomicInteger[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      counters[i] = new AtomicInteger(0);
    }
  }

  public int getStripeCount() {
    return counters.length;
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public RefCounterReleaser acquire() {
    final AtomicInteger counter = counters[getStripeIdx()];
    if (!addToCounter(counter, Delta.PLUS_ONE)) {
      // Counting is in progress, so we need to get a lock which will likely block
      // until the count is complete
      synchronized (this) {
        if (!addToCounter(counter, Delta.PLUS_ONE)) {
          throw new IllegalStateException("Count should not be in progress while we hold the lock");
        }
      }
    }
    return new RefCounterReleaserImpl(this, counter);
  }

  private static int getDefaultStripeCount() {
    return Math.min(
        MAX_STRIPES,
        Math.max(
            lowestPowerOfTwoGreaterThanOrEqualTo(PROCESSOR_COUNT * 2),
            DEFAULT_STRIPES));
  }

  /**
   * Returns the lowest power of two that is greater than or equal to {@code value}.
   *
   * @param value input value, must be positive
   * @return lowest power of two >= value
   * @throws IllegalArgumentException if {@code value <= 0} or the result would overflow an int
   */
  static int lowestPowerOfTwoGreaterThanOrEqualTo(final int value) {
    if (value <= 0) {
      throw new IllegalArgumentException("Value must be positive, got: " + value);
    }
    if (value > (1 << 30)) {
      throw new IllegalArgumentException(
          "Value is too large to round up to a positive int power of two, got: " + value);
    }
    return value == 1
        ? 1
        : Integer.highestOneBit(value - 1) << 1;
  }

  private void release(final AtomicInteger counter) {
    if (!addToCounter(counter, Delta.MINUS_ONE)) {
      synchronized (this) {
        if (!addToCounter(counter, Delta.MINUS_ONE)) {
          throw new IllegalStateException("Count should not be in progress while we hold the lock");
        }
      }
    }
  }

  @Override
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);

    // close is idempotent so silently drop out
    if (isClosed.get()) {
      return;
    }

    synchronized (this) {
      if (isClosed.get()) {
        return;
      }

      // Once we have marked all counters as count-in-progress, any threads trying to mutate the counters
      // will fail, then re-attempt under lock, so will have to wait for us to complete the count.
      // Marking all the counters is a non-atomic operation, so another thread may increment a counter
      // while we are in the middle of marking them, however, once all are marked, threads will be blocked
      // from decrementing until we have called markCountersAsNoCountInProgress(), thus we will get a non-zero
      // count and throw an EnvInUseException.

      markCountersAsCountInProgress(); // 0=>MAGIC_ZERO_VALUE else i=>i*-1

      // At this point, no other thread can mutate the counters, so we are safe to use a sum of all the counters.
      try {
        final long totalCount = sumCounters();
        if (totalCount == 0) {
          // No permits on loan so safe to close.
          onClose.run();
          // Only mark as closed if the runnable succeeds.
          isClosed.set(true);
          // Mark all counters as closed to prevent any future acquire() calls
          for (final AtomicInteger counter : counters) {
            counter.set(MAGIC_CLOSED_VALUE);
          }
        } else {
          throw new Env.EnvInUseException(totalCount);
        }
      } finally {
        if (!isClosed.get()) {
          // Return all counters to their original positive values so
          // acquire/release can resume as normal
          markCountersAsNoCountInProgress(); // MAGIC_ZERO_VALUE=>0 else i=>i*-1
        }
      }
    }
  }

  /**
   * MUST be called after {@link StripedRefCounter#markCountersAsCountInProgress()} has been called and under
   * lock. Once complete, {@link StripedRefCounter#markCountersAsNoCountInProgress()} must be called.
   */
  private long sumCounters() {
    long totalCount = 0;
    for (AtomicInteger counter : counters) {
      int count = counter.get();
      if (count == MAGIC_CLOSED_VALUE) {  // Integer.MAX_VALUE
        throw new Env.AlreadyClosedException();
      } else if (count != MAGIC_ZERO_VALUE) {  // Integer.MIN_VALUE
        // count should be negative at this point
        if (count > 0) {
          throw new IllegalStateException("Count should be negative at this point, got: " + count);
        }
        totalCount += count;
      }
    }
    // The individual counts were all negative, so use the abs value
    totalCount = Math.abs(totalCount);
    return totalCount;
  }

  @Override
  public long getCount() {
    if (isClosed()) {
      return 0;
    }
    synchronized (this) {
      if (isClosed()) {
        return 0;
      }
      // This will stop any other thread from incrementing/decrementing the counter
      markCountersAsCountInProgress();
      try {
        return sumCounters();
      } finally {
        markCountersAsNoCountInProgress();
      }
    }
  }

  /**
   * @return False if a count is in progress, else true
   * @throws Env.AlreadyClosedException If this {@link RefCounter} has already been
   *                                    successfully closed.
   */
  private boolean addToCounter(final AtomicInteger counter, final Delta delta) {
    // Use a while loop with get() and compareAndSet(), rather than throwing exceptions inside
    // updateAndGet().
    while (true) {
      final int currVal = counter.get();

      if (currVal == MAGIC_CLOSED_VALUE) {
        // Once MAGIC_CLOSED_VALUE is set, it is never mutated again.
        throw new Env.AlreadyClosedException();
      } else if (currVal < 0) {
        // A count is in progress, so we can drop out and try again under lock
        return false;
      } else if (currVal == MAX_COUNTER_VALUE && delta == Delta.PLUS_ONE) {
        // This implies we have a LOT of txns/cursors open, should never happen
        throw new IllegalStateException("Reference count overflow");
      } else if (currVal == 0 && delta == Delta.MINUS_ONE) {
        throw new IllegalStateException("Reference count underflow");
      }

      final int newVal = currVal + delta.deltaValue;
      if (counter.compareAndSet(currVal, newVal)) {
        return true;
      }
    }
  }

  /**
   * Must be called while holding the lock on this object.
   */
  private void markCountersAsNoCountInProgress() {
    for (AtomicInteger counter : counters) {
      // Multiply value by -1 so we can indicate to other threads that a count is in progress
      // while maintaining the count. Have to use a special replacement value for zero.
      counter.updateAndGet(currVal -> {
        if (currVal == MAGIC_ZERO_VALUE) {
          return 0;
        } else if (currVal == MAGIC_CLOSED_VALUE) {
          // If this method is used correctly under lock, we should never see this value, but preserve the
          // closed state just in case
          return MAGIC_CLOSED_VALUE;
        } else {
          return Math.abs(currVal);
        }
      });
    }
  }

  /**
   * Must be called while holding the lock on this object.
   */
  private void markCountersAsCountInProgress() {
    // It is possible that another thread will call acquire() while we are mid-loop.
    // If that thread uses a counter that has not yet been marked as count-in-progress, they will
    // succeed with incrementing the counter.
    // We will then get a sum that includes the increment from their acquire() call.
    // They will be blocked from calling release() until markCountersAsNoCountInProgress() has
    // been called by us.
    for (AtomicInteger counter : counters) {
      counter.updateAndGet(currVal -> {
        if (currVal == 0) {
          // Use a magic value to mark this zero-value counter as having a count in progress
          return MAGIC_ZERO_VALUE;
        } else if (currVal == MAGIC_CLOSED_VALUE) {
          // If this method is used correctly under lock, we should never see this value, but preserve the
          // closed state just in case
          return MAGIC_CLOSED_VALUE;
        } else {
          // Make the value negative to indicate a count in progress
          return Math.abs(currVal) * -1;
        }
      });
    }
  }

  private void validateStripeCount(final int stripeCount) {
    if (stripeCount <= 0) {
      throw new IllegalArgumentException(
          "Stripe count must be positive, got: " + stripeCount);
    }
    if (stripeCount > MAX_STRIPES) {
      throw new IllegalArgumentException(
          "Stripe count exceeds maximum. Got: " + stripeCount +
              ", max: " + MAX_STRIPES);
    }
    if ((stripeCount & (stripeCount - 1)) != 0) {
      throw new IllegalArgumentException(
          "Stripe count must be power of 2, got: " + stripeCount);
    }
  }

  /**
   * Computes the stripe index for the current thread using Stafford variant 13 mixing.
   * <p>
   * This method applies a high-quality 64-bit hash function (MurmurHash3 finalizer)
   * to the thread ID before masking to the stripe count. This provides:
   * <ul>
   *   <li>Excellent distribution for sequential thread IDs</li>
   *   <li>Same thread always maps to same stripe (deterministic)</li>
   *   <li>Strong avalanche properties (input bit changes affect all output bits)</li>
   *   <li>O(1) performance</li>
   * </ul>
   * <p>
   * The Stafford13 mixing function is used internally by {@link java.util.SplittableRandom}
   * for seed initialization. See:
   * <a href="http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html">
   * Better Bit Mixing</a>
   *
   * @return stripe index from 0 to stripeCount - 1 (inclusive)
   */
  private int getStripeIdx() {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    long threadId = Thread.currentThread().getId();
    // Stafford13 for sequential inputs
    threadId = (threadId ^ (threadId >>> 30)) * 0xbf58476d1ce4e5b9L;
    threadId = (threadId ^ (threadId >>> 27)) * 0x94d049bb133111ebL;
    return (int) ((threadId ^ (threadId >>> 31)) & stripeMask);
  }

  private static class RefCounterReleaserImpl implements RefCounterReleaser {

    private final AtomicReference<StripedRefCounter> refCounterRef;
    private final AtomicInteger counter;

    private RefCounterReleaserImpl(final StripedRefCounter refCounter,
                                   final AtomicInteger counter) {
      this.refCounterRef = new AtomicReference<>(refCounter);
      this.counter = counter;
    }

    @Override
    public void release() {
      // Prevent duplicate release calls
      final StripedRefCounter refCounter = refCounterRef.getAndSet(null);
      if (refCounter != null) {
        refCounter.release(counter);
      }
    }
  }

  private enum Delta {
    PLUS_ONE(1),
    MINUS_ONE(-1),
    ;

    private final int deltaValue;

    Delta(int deltaValue) {
      this.deltaValue = deltaValue;
    }
  }

  private static class Stripe {
    private final StripedRefCounter stRefCounter;
    private final AtomicInteger counter;

    Stripe(StripedRefCounter stRefCounter) {
      this.stRefCounter = stRefCounter;
      this.counter = new AtomicInteger();
    }
  }
}
