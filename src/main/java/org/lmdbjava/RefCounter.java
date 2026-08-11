/*
 * Copyright © 2016-2026 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

/**
 * Used to prevent the closure of a resource while other threads are actively using that resource.
 * Achieves this via reference counting.
 */
interface RefCounter {

  /**
   * Call this before using the {@link RefCounter} controlled object.
   *
   * @return A {@link RefCounterReleaser} to release once the work is complete
   */
  RefCounterReleaser acquire();

  /**
   * Calls {@link RefCounter#acquire()}, runs runnable, then calls {@link
   * RefCounterReleaser#release()}.
   *
   * <p>If runnable is null, this is a no-op.
   */
  default void use(final Runnable runnable) {
    if (runnable != null) {
      final RefCounterReleaser releaser = acquire();
      try {
        runnable.run();
      } finally {
        releaser.release();
      }
    }
  }

  /**
   * If the reference count is zero, onClose will be called. This {@link RefCounter} will be marked
   * as closed so all future calls to acquire will throw a {@link
   * org.lmdbjava.Env.AlreadyClosedException}. If the count is non-zero, {@link
   * org.lmdbjava.Env.EnvInUseException} will be thrown. If already closed, this is a no-op.
   *
   * @throws org.lmdbjava.Env.EnvInUseException If the {@link Env} has open transactions/cursors.
   */
  void close(final Runnable onClose);

  /**
   * If the reference count is zero, onClose will be called and true returned. This {@link
   * RefCounter} will be marked as closed so all future calls to acquire will throw a {@link
   * org.lmdbjava.Env.AlreadyClosedException}. If the count is non-zero it is a no-op and false is
   * returned. If already closed, this is a no-op and false is returned.
   *
   * @return True if onClose was called.
   */
  boolean tryClose(final Runnable onClose);

  /**
   * @return True if {@link RefCounter} has been closed.
   */
  boolean isClosed();

  /** If it is in a CLOSED state, throw a {@link org.lmdbjava.Env.AlreadyClosedException} */
  default void checkNotClosed() {
    if (isClosed()) {
      throw new Env.AlreadyClosedException();
    }
  }

  /**
   * @return The current count of items in use. It will return 0 if already closed.
   */
  long getCount();

  @FunctionalInterface
  interface RefCounterReleaser {

    /**
     * Call this after using the {@link RefCounter} controlled object. Subsequent calls to this
     * method are a no-op.
     */
    void release();
  }
}
