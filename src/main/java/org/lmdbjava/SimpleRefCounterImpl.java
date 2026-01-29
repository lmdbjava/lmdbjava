package org.lmdbjava;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class SimpleRefCounterImpl implements RefCounter {
  private final AtomicInteger counter;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean preventAcquire = new AtomicBoolean(false);

  public SimpleRefCounterImpl() {
    this.counter = new AtomicInteger(0);
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
      final int count = getCount();
      if (count == 0) {
        break;
      }
    }
  }

  @Override
  public void doWhenIdle(final Runnable runnable) {
    while (true) {
      final int count = getCount();
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
        preventAcquire.compareAndSet(false, true);
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

  public RefCounterReleaser acquire() {
    if (preventAcquire.get()) {
      throw new Env.AlreadyClosedException();
    }
    counter.incrementAndGet();
    return this::release;
  }

  private void release() {
    // Increment if greater than 0.
    counter.decrementAndGet();
  }

  public int getCount() {
    return  counter.get();
  }
}
