package org.lmdbjava;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

class StampedLockRefCounterImpl implements RefCounter {
  private static final int DEFAULT_STRIPES = 32;
  private static final int MAX_STRIPES = 256;

  private final StampedLock stampedLock;
  private final int stripes;
  private final AtomicInteger[] counters;
  private final AtomicBoolean closed = new AtomicBoolean(false);
//  private final AtomicBoolean preventAcquire = new AtomicBoolean(false);
  /**
   * Bit mask for fast stripe index calculation. Equal to (stripeCount - 1).
   * Used with bitwise AND for O(1) hashing with no modulo operation.
   */
  private final int stripeMask;


  public StampedLockRefCounterImpl() {
    this(DEFAULT_STRIPES);
  }

  public StampedLockRefCounterImpl(final int stripeCount) {
    this.stampedLock = new StampedLock();
    this.stripes = validateStripes(stripeCount);
    this.stripeMask = stripeCount - 1;
    this.counters = new AtomicInteger[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      counters[i] = new AtomicInteger(0);
    }
  }

  private int validateStripes(final int stripeCount) {
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
    return stripeCount;
  }

  /**
   * Computes the stripe index for the current thread using XOR-fold hashing.
   * <p>
   * This method combines the upper and lower 32 bits of the thread ID using XOR,
   * then masks to the stripe count using bitwise AND. This provides:
   * <ul>
   *   <li>Even distribution across stripes</li>
   *   <li>Same thread always maps to same stripe</li>
   *   <li>Guaranteed non-negative result</li>
   *   <li>O(1) performance (~3ns)</li>
   * </ul>
   *
   * @return stripe index in range [0, stripeCount), guaranteed non-negative
   */
  private int getStripeIdx() {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    final long threadId = Thread.currentThread().getId();

    // TODO In >= Java17 might as well replace with a JDK method
    //  final int mixedThreadId = (int) RandomSupport.mixStafford13(threadId);
    // bit-mixer/finalizer for improving the entropy of the threadId
    final int mixedThreadId = mixBits(threadId);
    return mixedThreadId & stripeMask;
  }


  /**
   * Returns the 32 high bits of David Stafford's variant 4 mix64 function as int.
   * <a href="http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html">
   *   better-bit-mixing-improving-on
   *   </a>
   * An evolution of the MurmurHash3 finalizer.
   */
  private static int mixBits(long val) {
    val = (val ^ (val >>> 33)) * 0x62a9d9ed799705f5L;
    return (int)(((val ^ (val >>> 28)) * 0xcb24d0a5c88c35b3L) >>> 32);
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
    while (true) {
      final long writeLockStamp = stampedLock.writeLock();
      try {
        final int count = getCountInternal();
        if (count == 0) {
          break;
        }
      } finally {
        stampedLock.unlockWrite(writeLockStamp);
      }
    }
  }

  @Override
  public void doWhenIdle(final Runnable runnable) {
    while (true) {
      final long writeLockStamp = stampedLock.writeLock();
      try {
        final int count = getCountInternal();
//        System.out.printf("%s - doWhenIdle() under writeLock, count: %s%n",
//            Thread.currentThread(), count);
        if (count == 0) {
          System.out.printf("%s - doWhenIdle() under writeLock, count: %s%n",
              Thread.currentThread(), count);
          if (closed.compareAndSet(false, true)) {
            runnable.run();
          }
          break;
        } else {
//          preventAcquire.compareAndSet(false, true);
        }
      } finally {
        stampedLock.unlockWrite(writeLockStamp);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
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
//    if (preventAcquire.get()) {
//      throw new Env.AlreadyClosedException();
//    }
    final long optimisticLockStamp = stampedLock.tryOptimisticRead();
    final int stripeIdx = getStripeIdx();
    final AtomicInteger counter = counters[stripeIdx];
    counter.incrementAndGet();

    if (!stampedLock.validate(optimisticLockStamp)) {
      // Undo incrementAndGet
      counter.decrementAndGet();

      // Now repeat under lock
      final long readLockStamp = stampedLock.readLock();
//        System.out.printf("%s - acquire() under readlock%n", Thread.currentThread());
      try {
        counter.incrementAndGet();
      } finally {
        stampedLock.unlockRead(readLockStamp);
      }
    }
    return () -> release(counter);
  }

  private void release(final AtomicInteger counter) {
    // Try an optimistic non-blocking read on the assumption that the
    // write lock in getCount() is not called very often and to limit
    // overhead on release
    final long optimisticLockStamp = stampedLock.tryOptimisticRead();

    counter.decrementAndGet();

    if (!stampedLock.validate(optimisticLockStamp)) {
      // Optimistic read not successful, so undo our change and try again
      // with a proper read lock that may block
      counter.incrementAndGet();

      // Now repeat under lock
      final long readLockStamp = stampedLock.readLock();
//      System.out.printf("%s - release() under readlock%n", Thread.currentThread());
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
      return getCountInternal();
    } finally {
      stampedLock.unlockWrite(writeLockStamp);
    }
  }

  private int getCountInternal() {
    int count = 0;
    for (int i = 0; i < stripes; i++) {
      count += counters[i].get();
    }
    return count;
  }
}
