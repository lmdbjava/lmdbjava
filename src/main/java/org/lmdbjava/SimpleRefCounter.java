package org.lmdbjava;


import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

class SimpleRefCounter implements RefCounter {
  private static final int CLOSED_VALUE = Integer.MIN_VALUE;
  private final AtomicInteger counter = new AtomicInteger(0);

  @Override
  public boolean isClosed() {
    return counter.get() == CLOSED_VALUE;
  }

  public RefCounterReleaser acquire() {
    final int newVal = counter.updateAndGet(currVal ->
        currVal == CLOSED_VALUE
            ? currVal
            : currVal + 1);
    if (newVal == CLOSED_VALUE) {
      throw new Env.AlreadyClosedException();
    }
    return this::release;
  }

  @Override
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);
    if (counter.get() != CLOSED_VALUE) {
      // Set to CLOSED_VALUE to indicate closure, if the count is 0
      if (counter.compareAndSet(0, CLOSED_VALUE)) {
        onClose.run();
      } else {
        throw new Env.EnvInUseException(getCount());
      }
    }
  }

  private void release() {
    final int newVal = counter.updateAndGet(currVal ->
        currVal == CLOSED_VALUE
            ? currVal
            : currVal - 1);
    if (newVal == CLOSED_VALUE) {
      throw new Env.AlreadyClosedException();
    }
  }

  @Override
  public int getCount() {
    return Math.max(0, counter.get());
  }
}
