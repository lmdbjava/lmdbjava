package org.lmdbjava;


public class NoOpRefCounter implements RefCounter {

  public NoOpRefCounter() {
  }

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
    // Close with no checks
    onClose.run();
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void checkNotClosed() {
    // no-op
  }

  @Override
  public int getCount() {
    return 0;
  }

}
