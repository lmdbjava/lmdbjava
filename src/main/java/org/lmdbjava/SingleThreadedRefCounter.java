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

/** A {@link RefCounter} intented for use only in single-threaded environments. */
public class SingleThreadedRefCounter implements RefCounter {

  private int refCount;
  private boolean isClosed = false;

  public SingleThreadedRefCounter() {}

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
    if (runnable != null) {
      final RefCounterReleaser releaser = acquire();
      try {
        runnable.run();
      } finally {
        releaser.release();
      }
    }
  }

  @Override
  public void close(final Runnable onClose) {
    Objects.requireNonNull(onClose);
    if (!isClosed) {
      final long count = getCount();
      if (count == 0) {
        isClosed = true;
        onClose.run();
      } else {
        throw new Env.EnvInUseException(count);
      }
    }
  }

  @Override
  public boolean tryClose(Runnable onClose) {
    Objects.requireNonNull(onClose);
    if (!isClosed) {
      final long count = getCount();
      if (count == 0) {
        isClosed = true;
        onClose.run();
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public long getCount() {
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
