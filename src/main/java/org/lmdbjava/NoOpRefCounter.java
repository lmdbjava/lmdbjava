package org.lmdbjava;


import java.util.Objects;

public class NoOpRefCounter implements RefCounter {

  private final Runnable onClose;

  public NoOpRefCounter(final Runnable onClose) {
    this.onClose = Objects.requireNonNull(onClose);
  }

  @Override
  public RefCounterReleaser acquire() {
    return RefCounterReleaser.NO_OP_RELEASER;
  }

  @Override
  public void use(Runnable runnable) {
    runnable.run();
  }

  @Override
  public void close() {
    onClose.run();
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
    // no-op
  }

  @Override
  public void checkOpen() {
    // no-op
  }
}
