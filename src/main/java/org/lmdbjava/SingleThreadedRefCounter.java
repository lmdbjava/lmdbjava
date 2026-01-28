package org.lmdbjava;


import java.util.Objects;

public class SingleThreadedRefCounter implements RefCounter {

  private final Runnable onClose;
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
    if (envState ==  EnvState.OPEN) {
      envState = EnvState.CLOSING;
    }

    if (refCount > 0) {
      throw new Env.EnvInUseException();
    }

    if (envState == EnvState.CLOSING) {
      onClose.run();
      envState = EnvState.CLOSED;
    }
  }

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
    if (envState == EnvState.CLOSED) {
      throw new Env.AlreadyClosedException();
    }
  }

  @Override
  public void checkOpen() {
    if (envState != EnvState.OPEN) {
      throw new Env.AlreadyClosedException();
    }
  }

  @Override
  public int getCount() {
    return refCount;
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
