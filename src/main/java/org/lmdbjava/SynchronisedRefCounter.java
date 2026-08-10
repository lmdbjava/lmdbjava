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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link RefCounter} that uses synchronisation to track the number of
 * references to a resource.
 */
class SynchronisedRefCounter implements RefCounter {
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
    final AtomicBoolean hasReleased = new AtomicBoolean(false);
    return () -> {
      // Prevent duplicate release calls
      if (hasReleased.compareAndSet(false, true)) {
        release();
      }
    };
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
  public long getCount() {
    synchronized (this) {
      return counter;
    }
  }
}
