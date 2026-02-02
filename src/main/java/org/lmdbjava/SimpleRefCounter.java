package org.lmdbjava;


import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class SimpleRefCounter implements RefCounter {
  private final AtomicInteger counter;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public SimpleRefCounter() {
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
    if (isClosed.get()) {
      throw new Env.AlreadyClosedException();
    }
    counter.updateAndGet(currVal -> {
      if (currVal < 0) {
        throw new Env.AlreadyClosedException();
      } else {
        return currVal + 1;
      }
    });
    return this::release;
  }

  @Override
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);
    if (!isClosed.get()) {
      final int count = getCount();
      if (count == 0) {
        // Set to -1 to indicate closure
        if (counter.compareAndSet(count, -1)) {
          if (isClosed.compareAndSet(false, true)) {
            onClose.run();
          }
        } else {
          throw new Env.EnvInUseException(getCount());
        }
      } else {
        throw new Env.EnvInUseException(count);
      }
    }
  }

  private void release() {
    if (isClosed.get()) {
      throw new Env.AlreadyClosedException();
    }
    counter.decrementAndGet();
  }

  @Override
  public int getCount() {
    return counter.get();
  }
}
