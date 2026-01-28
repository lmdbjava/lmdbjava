package org.lmdbjava;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class Guard {
  private static final long GOLDEN_RATIO = 0x9e3779b9L;

  private final int stripes;
  private final AtomicInteger[] inUseCounts;
  private final AtomicBoolean destroy = new AtomicBoolean();
  //    private final AtomicBoolean destroyed = new AtomicBoolean();
  private final AtomicBitSet bitSet;
  private final Runnable destroyRunnable;

  public Guard(final Runnable destroyRunnable, final int stripes) {
    this.stripes = stripes;
    this.destroyRunnable = destroyRunnable;
    this.inUseCounts = new AtomicInteger[stripes];
    this.bitSet = new AtomicBitSet(stripes);
    for (int i = 0; i < stripes; i++) {
      inUseCounts[i] = new AtomicInteger(1);
    }
  }

  private int getStripeIdx() {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    final long idx = (Thread.currentThread().getId() * GOLDEN_RATIO) % stripes;
    return (int) idx;
  }

  public <R> R acquire(final Supplier<R> supplier) {
    // Increment if greater than 0.
    final int c = inUseCounts[getStripeIdx()].updateAndGet(count -> count > 0
        ? count + 1
        : count);
    if (c <= 0) {
      // The destroy flag may not have been set when we entered this method but count == 0 means destruction
      // has been triggered since then.
      throw new RuntimeException("Try again");
    }

    try {
      return supplier.get();
    } finally {
      release();
    }
  }

  private void release() {
    // Decrement but don't go lower than 0.
    final int stripeIdx = getStripeIdx();
    release(stripeIdx);
  }

  private void release(final int stripeIdx) {
    // Decrement but don't go lower than 0.
    final int newCount = inUseCounts[stripeIdx].updateAndGet(count -> {
      if (count > 0) {
        return count - 1;
      } else if (count < 0) {
        return count + 1;
      } else {
        return count;
      }
    });

    if (newCount == 0) {
      if (!bitSet.isSet(stripeIdx)) {
        final long prevVal = bitSet.getAndSet(stripeIdx);
        final boolean didChange = !bitSet.isSet(prevVal, stripeIdx);
        if (didChange) {
          if (bitSet.countUnSet(prevVal) == 1) {
            destroyRunnable.run();
          }
        }
      }
    }
  }

  public void destroy() {
    if (destroy.compareAndSet(false, true)) {
      // Perform final decrement. Close is either performed now if the guard is not acquired or will be
      // performed by the final thread that releases the acquisition.
      for (int stripeIdx = 0; stripeIdx < stripes; stripeIdx++) {
        release(stripeIdx);
      }
    } else {
//        LOGGER.debug("Guard already destroyed");
    }
  }
}
