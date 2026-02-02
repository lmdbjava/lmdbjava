package org.lmdbjava;

import java.util.concurrent.atomic.AtomicBoolean;

public class NoOpRefCounter implements RefCounter {

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Override
  public RefCounterReleaser acquire() {
    return RefCounter.NO_OP_RELEASER;
  }

  @Override
  public void use(Runnable runnable) {
    runnable.run();
  }

  @Override
  public void close(Runnable onClose) {
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
  public int getCount() {
    return 0;
  }
}
