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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Preforms no reference counting at all, but will throw an Env.AlreadyClosedException if the {@link
 * Env} is closed when {@link NoOpRefCounter#acquire()} is called.
 */
public class NoOpRefCounter implements RefCounter {

  /** A {@link RefCounterReleaser} that does nothing. */
  private static final RefCounterReleaser NO_OP_RELEASER =
      () -> {
        // No-op
      };

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Override
  public RefCounterReleaser acquire() {
    return NO_OP_RELEASER;
  }

  @Override
  public void use(final Runnable runnable) {
    runnable.run();
  }

  @Override
  public void close(final Runnable onClose) {
    if (isClosed.compareAndSet(false, true)) {
      // Close with no checks
      onClose.run();
    }
  }

  @Override
  public boolean tryClose(Runnable onClose) {
    if (isClosed.compareAndSet(false, true)) {
      // Close with no checks
      onClose.run();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public long getCount() {
    return 0;
  }
}
