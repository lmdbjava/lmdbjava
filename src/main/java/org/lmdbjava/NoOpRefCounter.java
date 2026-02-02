package org.lmdbjava;

public class NoOpRefCounter implements RefCounter {

  private boolean isClosed = false;

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
    isClosed = true;
    // Close with no checks
    onClose.run();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public int getCount() {
    return 0;
  }
}
