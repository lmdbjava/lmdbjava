package org.lmdbjava;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Preforms no reference counting at all, but will throw a Env.AlreadyClosedException
 * if the Env is closed when {@link NoOpRefCounter#acquire()} is called.
 */
public class NoOpRefCounter implements RefCounter {

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Override
  public RefCounterReleaser acquire() {
    if (isClosed.get()) {
      throw new Env.AlreadyClosedException();
    }

    return RefCounter.NO_OP_RELEASER;
  }

  @Override
  public void use(final Runnable runnable) {
    runnable.run();
  }

  @Override
  public void close(final Runnable onClose) {
    if (isClosed.compareAndSet(false, true)) {
      // Close with no checks
      onClose.run();
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public long getCount() {
    return 0;
  }
}
