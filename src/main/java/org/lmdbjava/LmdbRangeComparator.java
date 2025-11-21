/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
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

import static org.lmdbjava.Library.LIB;

import java.util.Objects;
import jnr.ffi.Pointer;

/**
 * Calls down to mdb_cmp to make use of the comparator that LMDB uses for insertion order. Has a
 * very slight overhead as compared to {@link CursorIterable.JavaRangeComparator}.
 */
class LmdbRangeComparator<T> implements RangeComparator {

  private final Pointer txnPointer;
  private final Pointer dbiPointer;
  private final Pointer cursorKeyPointer;
  private final Key<T> startKey;
  private final Key<T> stopKey;
  private final Pointer startKeyPointer;
  private final Pointer stopKeyPointer;

  public LmdbRangeComparator(
      final Txn<T> txn,
      final Dbi<T> dbi,
      final Cursor<T> cursor,
      final KeyRange<T> range,
      final BufferProxy<T> proxy) {
    txnPointer = Objects.requireNonNull(txn).pointer();
    dbiPointer = Objects.requireNonNull(dbi).pointer();
    cursorKeyPointer = Objects.requireNonNull(cursor).keyVal().pointerKey();
    // Allocate buffers for use with the start/stop keys if required.
    // Saves us copying bytes on each comparison
    Objects.requireNonNull(range);
    startKey = createKey(range.getStart(), proxy);
    stopKey = createKey(range.getStop(), proxy);
    startKeyPointer = startKey != null ? startKey.pointer() : null;
    stopKeyPointer = stopKey != null ? stopKey.pointer() : null;
  }

  @Override
  public int compareToStartKey() {
    return LIB.mdb_cmp(txnPointer, dbiPointer, cursorKeyPointer, startKeyPointer);
  }

  @Override
  public int compareToStopKey() {
    return LIB.mdb_cmp(txnPointer, dbiPointer, cursorKeyPointer, stopKeyPointer);
  }

  @Override
  public void close() {
    if (startKey != null) {
      startKey.close();
    }
    if (stopKey != null) {
      stopKey.close();
    }
  }

  private Key<T> createKey(final T keyBuffer, final BufferProxy<T> proxy) {
    if (keyBuffer != null) {
      final Key<T> key = proxy.key();
      key.keyIn(keyBuffer);
      return key;
    } else {
      return null;
    }
  }
}
