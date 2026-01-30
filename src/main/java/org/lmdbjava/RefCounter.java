package org.lmdbjava;

/**
 * Used to prevent the closure of a thing while other threads are actively
 * using that thing.
 */
interface RefCounter {

  RefCounterReleaser NO_OP_RELEASER = () -> {
    // No-op
  };

  /**
   * Call this before using the {@link RefCounter} controlled object.
   * @return A {@link RefCounterReleaser} to release once the work is complete
   */
  RefCounterReleaser acquire();

  /**
   * Calls {@link RefCounter#acquire()}, runs runnable, then calls {@link RefCounterReleaser#release()}
   */
  default void use(final Runnable runnable) {
    if (runnable != null) {
      final RefCounterReleaser releaser = acquire();
      try {
        runnable.run();
      }  finally {
        releaser.release();
      }
    }
  }

  /**
   * If the reference count is zero, onClose will be called. This {@link RefCounter} will be marked
   * as closed so all future calls to acquire will throw a {@link org.lmdbjava.Env.AlreadyClosedException}.
   * If the count is non-zero, {@link org.lmdbjava.Env.EnvInUseException} will be thrown.
   * @throws org.lmdbjava.Env.EnvInUseException If the {@link Env} has open transactions/cursors.
   * @throws org.lmdbjava.Env.AlreadyClosedException If this {@link RefCounter} has already been
   * successfully closed.
   */
  void close(final Runnable onClose);

  /**
   * @return True if {@link RefCounter} is in a state of {@link EnvState#CLOSED}
   */
  boolean isClosed();

  /**
   * If it is in a CLOSED state, throw a {@link org.lmdbjava.Env.AlreadyClosedException}
   */
  default void checkNotClosed() {
    if (isClosed()) {
      throw new Env.AlreadyClosedException();
    }
  }

  /**
   * @return The current count of items in use.
   * @throws org.lmdbjava.Env.AlreadyClosedException If called after it has been successfully closed.
   */
  int getCount();

  @FunctionalInterface
  interface RefCounterReleaser {

    void release();
  }
}
