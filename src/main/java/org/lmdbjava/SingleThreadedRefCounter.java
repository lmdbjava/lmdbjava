package org.lmdbjava;


import java.util.Objects;

public class SingleThreadedRefCounter implements RefCounter {

  private int refCount;
  private boolean isClosed = false;
  private EnvState envState;

  public SingleThreadedRefCounter() {
  }

  @Override
  public RefCounterReleaser acquire() {
    if (isClosed) {
      throw new Env.AlreadyClosedException();
    }
    refCount++;
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
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);
    if (!isClosed) {
      final int count = getCount();
      if (count == 0) {
        isClosed = true;
        onClose.run();
      } else {
        throw new Env.EnvInUseException(count);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
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
