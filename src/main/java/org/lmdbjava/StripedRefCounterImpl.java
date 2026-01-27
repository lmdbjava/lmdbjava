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
//  private final AtomicBoolean closeCalled = new AtomicBoolean(false);
  private final Runnable onClose;
//  private final AtomicBoolean onCloseCompleted = new AtomicBoolean(false);
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
//    return doWithOptionalLocking(() -> {
    if (stateRef.get() != EnvState.OPEN) {
//    if (closeCalled.get()) {
      // Close has been initiated, so acquire() is no longer allowed
//        System.out.println("Throwing AlreadyClosedException 1");
      throw new Env.AlreadyClosedException();
    }

    // close() may be called after we have checked closeCalled, but the updateAndGet
    // will ensure that we cannot increment the count if close() has been called.

    final StripeState stripeState = stripeStates[getStripeIdx()];
    // If we increment the count just before counter is made negative, then close()
    // will have to wait for us to release.
    final int count = stripeState.counter.updateAndGet(currVal -> {
      final int newVal;
      if (currVal < 0) {
        // Negative means it is in a closed state
//        System.out.println("Throwing AlreadyClosedException 2");
//        throw new Env.AlreadyClosedException();
        newVal = currVal;
      } else {
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

    if (count < 0) {
        throw new Env.AlreadyClosedException();
    }
    // Return the counter index, so the release call can use it
    return new RefCounterReleaserImpl(this, stripeState);
//    });
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
//    doWithOptionalLocking(() -> {
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
          // Reached zero, so set to the magic number
          newVal = CLOSED_COUNT;
          // We have reached zero, so count down the latch so that close() can stop blocking
//          stripeState.countDownLatch.countDown();
        }
      } else {
        // currVal == 0
        throw new IllegalStateException("currVal should never be zero on release()");
      }
      return newVal;
    });
//      return null;
//    });
  }

//  @Override
//  public void release(final int counterIdx) {
//    final StripeState stripeState = stripeStates[counterIdx];
//    release(stripeState);
//  }

  private void markClosing() {
    // Only want to do this once
    if (stateRef.compareAndSet(EnvState.OPEN, EnvState.CLOSING)) {
//    if (closeCalled.compareAndSet(false, true)) {
//      System.out.println("close() called");
      // Place each stripe into a closed state
      for (int stripe = 0; stripe < stripes; stripe++) {
        final StripeState stripeState = stripeStates[stripe];
        stripeState.counter.updateAndGet(currVal -> {
          if (currVal == 0) {
            // Count is already at zero so there will be nothing to wait for.
//            stripeState.countDownLatch.countDown();
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
    markClosing();

//    if (!onCloseCompleted.get()) {
    final EnvState envState = stateRef.get();
    if (envState == EnvState.CLOSING) {
      // TODO This will mark as closed, but then throw if it is still in use, which is not
      //  ideal.  It ought to throw before marking closed if in use.

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

//  @Override
//  public void close(long duration, TimeUnit timeUnit) {
//    markClosing();
//
////    if (!onCloseCompleted.get()) {
//    final EnvState envState = stateRef.get();
//    if (envState == EnvState.CLOSING) {
//      Duration totalWaitTime = Duration.ZERO;
//      // Now wait for all active threads to finish
//      for (int stripe = 0; stripe < stripes; stripe++) {
//        final Instant stripeStartTime = Instant.now();
//        final StripeState stripeState = stripeStates[stripe];
//        final AtomicInteger counter = stripeState.counter;
//        final CountDownLatch latch = stripeState.countDownLatch;
//
//        // By this point all counters will be negative, so we need to wait for them ALL to reach 0,
//        // except the ones with the magic value of CLOSED_COUNT
//
//        int count = counter.get();
//        if (count < 0 && count != CLOSED_COUNT) {
//          // Non-zero count so we must wait for it to hit 0
////          System.out.printf("%s - Waiting for closure, stripe: %s, count: %d, latch: %s%n",
////              Thread.currentThread(), stripe, count, latch.getCount());
//          try {
//            // Release will count down when it hits zero
//            final long latchCount = latch.getCount();
//            Instant now = Instant.now();
//            final boolean didCountDown = latch.await(duration, timeUnit);
//            if (!didCountDown) {
//              throw new Env.EnvInUseException();
//            }
//            count = counter.get();
//            if (counter.get() != 0) {
//              throw new IllegalStateException("count " + count
//                  + " should be zero at this point, latchCount: " + latchCount + ", new latchCount: " + latch.getCount()
//                  + ", waited: " + Duration.between(now, Instant.now()));
//            }
//          } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            // Swallow
//          }
//        } else if (count > 0) {
//          throw new IllegalStateException("count " + count + " should not positive");
//        }
//
//        totalWaitTime = totalWaitTime.plus(Duration.between(stripeStartTime, Instant.now()));
//      }
////      System.out.printf("%s - Wait complete, waited %s, count: %s%n",
////          Thread.currentThread(), totalWaitTime, getTotalCount());
//
//      // All counters returned to zero
//      onClose.run();
//      stateRef.set(EnvState.CLOSED);
//      onCloseCompleted.set(true);
////      System.out.printf("%s - onClose completed, waited %s, count: %s%n",
////          Thread.currentThread(), totalWaitTime, getTotalCount());
//    } else if (envState == EnvState.OPEN) {
//      throw new IllegalStateException("EnvState should not be OPEN at this point");
//    }
//  }

  private int getStripeIdx() {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    final long idx = Thread.currentThread().getId() % stripes;
    return (int) idx;
  }

  @Override
  public boolean isClosed() {
    // TODO May need a three state approach, i.e.
    //  OPEN - Open and ready for use.
    //  CLOSING - Not yet closed, but
    //  CLOSED -
    // close() has been called, but it may not be fully closed yet, i.e. the close
    // action may not have run/finished.
//    return closeCalled.get();
    return stateRef.get() == EnvState.CLOSED;
  }

  @Override
  public EnvState getState() {
    return stateRef.get();
  }

  @Override
  public void checkNotClosed() {
//    if (closeCalled.get()) {
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
//    private final CountDownLatch countDownLatch;

    private StripeState(final int index) {
      this.index = index;
      this.counter = new AtomicInteger(0);
//      this.countDownLatch = new CountDownLatch(1);
    }

    int getIndex() {
      return index;
    }

    AtomicInteger getCounter() {
      return counter;
    }

//    CountDownLatch getCountDownLatch() {
//      return countDownLatch;
//    }

    @Override
    public String toString() {
      return "Stripe{" +
          "index=" + index +
          ", counter=" + counter +
//          ", countDownLatch=" + countDownLatch +
          '}';
    }
  }

//  private <T> T doWithOptionalLocking(final Supplier<T> supplier) {
//    final State state = stateRef.get();
//    if (state == State.CLOSED) {
//      throw new Env.AlreadyClosedException();
//    } else if (state == State.OPEN) {
//      return supplier.get();
//    } else {
//      synchronized (this) {
//        final State state2 = stateRef.get();
//        if (state2 == State.CLOSED) {
//          throw new Env.AlreadyClosedException();
//        } else if (state2 == State.OPEN) {
//          return supplier.get();
//        } else {
//          throw new IllegalStateException("Should not be in a sate of CLOSING with the lock held");
//        }
//      }
//    }
//  }

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
