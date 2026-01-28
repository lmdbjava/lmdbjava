package org.lmdbjava;


import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

class StampedLockRefCounterImpl implements RefCounter {
  private static final long GOLDEN_RATIO = 0x9e3779b9L;

  private final StampedLock stampedLock;
  private final int stripes;
  private final AtomicInteger[] counters;

  public StampedLockRefCounterImpl(final int stripes) {
    this.stampedLock = new StampedLock();
    this.stripes = stripes;
    this.counters = new AtomicInteger[stripes];
    for (int i = 0; i < stripes; i++) {
      counters[i] = new AtomicInteger(0);
    }
  }

  private int getStripeIdx() {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    final long idx = (Thread.currentThread().getId() * GOLDEN_RATIO) % stripes;
    return (int) idx;
  }

  @Override
  public void use(Runnable runnable) {
    acquire();
    try {
      runnable.run();
    } finally {
      release();
    }
  }

  @Override
  public void close() {

  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public EnvState getState() {
    return null;
  }

  @Override
  public void checkNotClosed() {

  }

  @Override
  public void checkOpen() {

  }

  public <R> R acquire(final Supplier<R> supplier) {
    acquire();
    try {
      return supplier.get();
    } finally {
      release();
    }
  }

  private void release() {
    final AtomicInteger counter = counters[getStripeIdx()];
    release(counter);
  }

  public RefCounterReleaser acquire() {
    final long optimisticLockStamp = stampedLock.tryOptimisticRead();
    final int stripeIdx = getStripeIdx();
    // Increment if greater than 0.
    final AtomicInteger counter = counters[stripeIdx];
    counter.incrementAndGet();

    final boolean success = stampedLock.validate(optimisticLockStamp);
    if (!success) {
      // Undo incrementAndGet
      counter.decrementAndGet();

      // Now repeat under lock
      final long readLockStamp = stampedLock.readLock();
      try {
        counter.incrementAndGet();
      } finally {
        stampedLock.unlockRead(readLockStamp);
      }
    }
    return () -> release(counter);
  }

  private void release(final AtomicInteger counter) {
    final long optimisticLockStamp = stampedLock.tryOptimisticRead();

    // Increment if greater than 0.
    counter.decrementAndGet();

    final boolean success = stampedLock.validate(optimisticLockStamp);
    if (!success) {
      // Undo decrementAndGet
      counter.incrementAndGet();

      // Now repeat under lock
      final long readLockStamp = stampedLock.readLock();
      try {
        counter.decrementAndGet();
      } finally {
        stampedLock.unlockRead(readLockStamp);
      }
    }
  }

  public int getCount() {
    final long writeLockStamp = stampedLock.writeLock();
    try {
      int count = 0;
      for (int i = 0; i < stripes; i++) {
        count += counters[i].get();
      }
      return count;
    } finally {
      stampedLock.unlockWrite(writeLockStamp);
    }
  }
}
