package org.lmdbjava;


import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class StripedRefCounterImpl implements RefCounter {
  private static final int CLOSED_COUNT = Integer.MIN_VALUE;

  /**
   * Number of stripes used to improve the concurrency
   */
  private final int stripes;
  private final StripeState[] stripeStates;
  /**
   * Flag to indicate if {@link RefCounter#close()} has been called.
   * Once set, it means that all subsequent {@link RefCounter#acquire()} will throw.
   */
  private final Runnable onClose;
  private final AtomicReference<EnvState> stateRef;

  StripedRefCounterImpl(Runnable onClose) {
    // Default to 1 stripe per processor for max concurrency
    this(Runtime.getRuntime().availableProcessors(), onClose);
  }

  StripedRefCounterImpl(int stripes, Runnable onClose) {
    this.stripes = stripes;
    if (stripes <= 0) {
      throw new IllegalArgumentException("stripes must be positive");
    }
    this.stripeStates = new StripeState[stripes];
    this.onClose = requireNonNull(onClose);
    this.stateRef =  new AtomicReference<>(EnvState.OPEN);
    for (int stripeIdx = 0; stripeIdx < stripes; stripeIdx++) {
      stripeStates[stripeIdx] = new StripeState(stripeIdx);
    }
  }

  @Override
  public RefCounterReleaser acquire() {
    if (stateRef.get() != EnvState.OPEN) {
      // Close has been initiated, so acquire() is no longer allowed
//        System.out.println("Throwing AlreadyClosedException 1");
      throw new Env.AlreadyClosedException();
    }

    // close() may be called after we have checked closeCalled, but the updateAndGet
    // will ensure that we cannot increment the count if close() has been called.

    final StripeState stripeState = stripeStates[getStripeIdx()];
    // If we increment the count just before counter is made negative, then close()
    // will have to wait for us to release.
    final int newCount = stripeState.counter.updateAndGet(currVal -> {
      int newVal = currVal;
      if (currVal >= 0) {
        newVal = currVal + 1;
//        System.out.printf("%s - acquire() called, currVal: %s, newVal: %s%n",
//            Thread.currentThread(), currVal, newVal);
        if (newVal == Integer.MAX_VALUE) {
          // MAX_VALUE is not allowed as that would become CLOSED_COUNT when made negative
          throw new IllegalStateException("Too many concurrent acquire calls");
        }
      }
      return newVal;
    });

    if (newCount < 0) {
        throw new Env.AlreadyClosedException();
    }
    // Return the releaser than knows which stripe to release back to
    return new RefCounterReleaserImpl(this, stripeState);
  }

  @Override
  public void use(final Runnable runnable) {
    if (runnable != null) {
      final RefCounterReleaser releaser = acquire();
      try {
        runnable.run();
      } finally {
        releaser.release();
      }
    }
  }

  private void release(final StripeState stripeState) {
    stripeState.counter.updateAndGet(currVal -> {
      int newVal;
      if (currVal > 0) {
        // Positive count, so in an open state, therefore -1 back down towards zero
        newVal = currVal - 1;
//        System.out.printf("%s - release() called, currVal: %s, newVal: %s%n",
//            Thread.currentThread(), currVal, newVal);
      } else if (currVal == CLOSED_COUNT) {
        // CLOSED_COUNT is only set if the value is zero on close()
        throw new IllegalStateException("currVal should never be CLOSED_COUNT on release()");
      } else if (currVal < 0) {
        // Negative count, so in a closed state
        // +1 to take the count back up towards zero
        newVal = currVal + 1;
//        System.out.printf("%s - release() called, currVal: %s, newVal: %s%n",
//            Thread.currentThread(), currVal, newVal);
        if (newVal == 0) {
          // Reached zero, so set to the magic number, so counter stays negative, i.e. closed
          newVal = CLOSED_COUNT;
        }
      } else {
        throw new IllegalStateException("currVal should never be zero on release()");
      }
      return newVal;
    });
  }

  private void setCountersInClosingState() {
    // Only want to do this once
    if (stateRef.compareAndSet(EnvState.OPEN, EnvState.CLOSING)) {
//      System.out.println("close() called");
      // Place each stripe into a closed state
      for (int stripe = 0; stripe < stripes; stripe++) {
        final StripeState stripeState = stripeStates[stripe];
        stripeState.counter.updateAndGet(currVal -> {
          if (currVal == 0) {
            // Count is already at zero so there will be nothing to wait for.
            // Ensures any thread that tries to increment will see it as closed
            return CLOSED_COUNT;
          } else if (currVal > 0) {
            // Make it negative to indicate the closed state but maintain the ref count
            // (albeit as a negative number)
            final int newVal = currVal * -1;
//            System.out.printf("%s - close() called, currVal: %s, newVal: %s%n",
//                Thread.currentThread(), currVal, newVal);
            return newVal;
          } else {
            throw new IllegalStateException("currVal should not be zero on close()");
          }
        });
      }
    }
  }

  @Override
  public void close() {
    // First ensure all counters are marked as closing to stop any new acquire calls
    setCountersInClosingState();

    // At this point, no new acquire calls are possible
    final EnvState envState = stateRef.get();
    if (envState == EnvState.CLOSING) {

      // If any counter is negative then there are still release() calls outstanding
      for (int stripe = 0; stripe < stripes; stripe++) {
        final StripeState stripeState = stripeStates[stripe];
        final int count = stripeState.counter.get();
        if (count < 0 && count != CLOSED_COUNT) {
          throw new Env.EnvInUseException(getTotalCount());
        }
      }

      onClose.run();
      stateRef.set(EnvState.CLOSED);
    } else if (envState == EnvState.OPEN) {
      throw new IllegalStateException("EnvState should not be OPEN at this point");
    }
  }

  private int getStripeIdx() {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    final long idx = Thread.currentThread().getId() % stripes;
    return (int) idx;
  }

  @Override
  public boolean isClosed() {
    // close() has been called, but it may not be fully closed yet, i.e. the close
    // action may not have run/finished.
    // TODO should it return ==CLOSED or !=OPEN ?
    return stateRef.get() == EnvState.CLOSED;
  }

  @Override
  public EnvState getState() {
    return stateRef.get();
  }

  @Override
  public void checkNotClosed() {
    // TODO should it return ==CLOSED or !=OPEN ?
    if (stateRef.get() !=  EnvState.OPEN) {
      throw new Env.AlreadyClosedException();
    }
  }

  /**
   * @return The total number of active users. Not atomic.
   */
  int getTotalCount() {
    return Arrays.stream(stripeStates)
        .map(StripeState::getCounter)
        .mapToInt(AtomicInteger::get)
        .filter(i -> i != CLOSED_COUNT)
        .map(Math::abs) // Count could be +ve/-ve so take abs value
        .sum();
  }

  private static class StripeState {

    private final int index;
    private final AtomicInteger counter;
    /**
     * One latch per stripe. Each will start with a value of 1
     */

    private StripeState(final int index) {
      this.index = index;
      this.counter = new AtomicInteger(0);
    }

    int getIndex() {
      return index;
    }

    AtomicInteger getCounter() {
      return counter;
    }

    @Override
    public String toString() {
      return "Stripe{" +
          "index=" + index +
          ", counter=" + counter +
          '}';
    }
  }

  private static class RefCounterReleaserImpl implements RefCounterReleaser {

    private final AtomicReference<StripedRefCounterImpl> refCounterRef;
    private final StripeState stripeState;

    private RefCounterReleaserImpl(final StripedRefCounterImpl refCounter,
                                   final StripeState stripeState) {
      this.refCounterRef = new AtomicReference<>(refCounter);
      this.stripeState = stripeState;
    }

    @Override
    public void release() {
      // Prevent duplicate release calls
      final StripedRefCounterImpl refCounter = refCounterRef.getAndSet(null);
      if (refCounter != null) {
          refCounter.release(stripeState);
      }
    }
  }
}
