package org.lmdbjava;


import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class SimpleRefCounterImpl implements RefCounter {
  private final AtomicInteger counter;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicBoolean preventAcquire = new AtomicBoolean(false);

  public SimpleRefCounterImpl() {
    this.counter = new AtomicInteger(0);
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
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

  @Override
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);
    if (!isClosed.get()) {
      final int count = getCount();
      if (count == 0) {
        if (isClosed.compareAndSet(false, true)) {
          onClose.run();
        }
      } else {
        throw new Env.EnvInUseException(count);
      }
    }
  }

  private void release() {
    // Increment if greater than 0.
    counter.decrementAndGet();
  }

  @Override
  public int getCount() {
    return counter.get();
  }
}
