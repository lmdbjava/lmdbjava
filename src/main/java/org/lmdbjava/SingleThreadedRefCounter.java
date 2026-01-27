package org.lmdbjava;


import java.util.Objects;

public class SingleThreadedRefCounter implements RefCounter {

  private final Runnable onClose;
  private boolean closeCalled = false;
  private boolean onCloseCompleted = false;
  private int refCount;
  private EnvState envState;

  public SingleThreadedRefCounter(final Runnable onClose) {
    this.onClose = Objects.requireNonNull(onClose);
    this.envState = EnvState.OPEN;
  }

  @Override
  public RefCounterReleaser acquire() {
    if (envState != EnvState.OPEN) {
      throw new Env.AlreadyClosedException();
    }
    return new SingleThreadedReleaser(this);
  }

  private void release() {
    if (refCount == 0) {
      throw new IllegalStateException("Attempt to release with a refCount of zero");
    }
    refCount--;
  }

  @Override
  public void use(Runnable runnable) {
    final RefCounterReleaser releaser = acquire();
    try {
      runnable.run();
    } finally {
      releaser.release();
    }
  }

  @Override
  public void close() {
    envState = EnvState.CLOSING;
//    closeCalled = true;
    if (refCount > 0) {
      throw new Env.EnvInUseException();
    }
//    if (!onCloseCompleted) {
    if (envState == EnvState.CLOSING) {
      onClose.run();
      envState = EnvState.CLOSED;
//      onCloseCompleted = true;
    }
  }

//  @Override
//  public void close(long duration, TimeUnit timeUnit) {
//    throw new UnsupportedOperationException("Method not supported for single threaded use.");
//  }

  @Override
  public boolean isClosed() {
    return envState == EnvState.CLOSED;
  }

  @Override
  public EnvState getState() {
    return envState;
  }

  @Override
  public void checkNotClosed() {
    if (envState != EnvState.OPEN) {
      throw new Env.AlreadyClosedException();
    }
  }

  private static class SingleThreadedReleaser implements RefCounterReleaser {

    private final SingleThreadedRefCounter singleThreadedRefCounter;
    private boolean released = false;

    private SingleThreadedReleaser(final SingleThreadedRefCounter singleThreadedRefCounter) {
      this.singleThreadedRefCounter = singleThreadedRefCounter;
    }

    @Override
    public void release() {
      if (!released) {
        released = true;
        singleThreadedRefCounter.release();
      }
    }
  }
}
