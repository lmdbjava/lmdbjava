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
import java.util.concurrent.atomic.AtomicInteger;

class SimpleRefCounter implements RefCounter {
  private static final int CLOSED_VALUE = Integer.MIN_VALUE;
  private final AtomicInteger counter = new AtomicInteger(0);

  @Override
  public boolean isClosed() {
    return counter.get() == CLOSED_VALUE;
  }

  public RefCounterReleaser acquire() {
    final int newVal =
        counter.updateAndGet(currVal ->
            currVal == CLOSED_VALUE ? currVal : currVal + 1);
    if (newVal == CLOSED_VALUE) {
      throw new Env.AlreadyClosedException();
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
    if (counter.get() != CLOSED_VALUE) {
      // Set to CLOSED_VALUE to indicate closure, if the count is 0
      if (counter.compareAndSet(0, CLOSED_VALUE)) {
        onClose.run();
      } else {
        throw new Env.EnvInUseException(getCount());
      }
    }
  }

  private void release() {
    final int newVal =
        counter.updateAndGet(currVal -> currVal == CLOSED_VALUE ? currVal : currVal - 1);
    if (newVal == CLOSED_VALUE) {
      throw new Env.AlreadyClosedException();
    }
  }

  @Override
  public long getCount() {
    return Math.max(0, counter.get());
  }
}
