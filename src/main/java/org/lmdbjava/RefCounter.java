package org.lmdbjava;


/**
 * Used to prevent the closure of a thing while other threads are actively
 * using that thing.
 */
interface RefCounter {

  /**
   * Call this before using the {@link RefCounter} controlled object.
   * @return A {@link RefCounterReleaser} to release once the work is complete
   */
  RefCounterReleaser acquire();

  /**
   * Calls {@link RefCounter#acquire()}, runs runnable, then calls {@link RefCounterReleaser#release()}
   */
  void use(final Runnable runnable);

  /**
   * @param counterIdx
   */
//  void release(int counterIdx);

  /**
   * Closes the {@link RefCounter} controlled item, but only after ensuring all active users of
   * it have released. Once {@link RefCounter#close()} is called, all subsequent calls to
   * {@link RefCounter#acquire()} or {@link RefCounter#use(Runnable)} will throw an
   * {@link org.lmdbjava.Env.AlreadyClosedException}
   * @throws org.lmdbjava.Env.EnvInUseException If the {@link Env} has open transactions/cursors.
   */
  void close();

//  void close(final long duration, final TimeUnit timeUnit);

  /**
   * @return True if {@link RefCounter#close()} has been called. The actual close may not have completed though.
   */
  boolean isClosed();

  EnvState getState();

  void checkNotClosed();

  @FunctionalInterface
  interface RefCounterReleaser {

    RefCounterReleaser NO_OP_RELEASER = () -> {
      // No-op
    };

    void release();
  }
}
