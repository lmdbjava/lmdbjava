package org.lmdbjava;


import java.util.Objects;

class SynchronisedRefCounter implements RefCounter {
  private static final int CLOSED_VALUE = Integer.MIN_VALUE;
  private boolean isClosed = false;
  private int counter = 0;

  @Override
  public boolean isClosed() {
    synchronized (this) {
      return isClosed;
    }
  }

  public RefCounterReleaser acquire() {
    synchronized (this) {
      if (isClosed) {
        throw new Env.AlreadyClosedException();
      }
      counter++;
    }
    return this::release;
  }

  @Override
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);
    synchronized (this) {
      if (!isClosed) {
        if (counter != 0) {
          throw new Env.EnvInUseException(getCount());
        } else {
          isClosed = true;
          onClose.run();
        }
      }
    }
  }

  private void release() {
    synchronized (this) {
      if (isClosed) {
        throw new Env.AlreadyClosedException();
      }
      if (counter == 0) {
        throw new IllegalStateException("Attempt to decrement counter below zero");
      }
      counter--;
    }
  }

  @Override
  public int getCount() {
    synchronized (this) {
      return counter;
    }
  }
}
