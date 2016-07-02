/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import java.util.Iterator;
import java.util.NoSuchElementException;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.CursorIterator.State.DONE;
import static org.lmdbjava.CursorIterator.State.NOT_READY;
import static org.lmdbjava.CursorIterator.State.READY;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;

/**
 * Iterator for entries that follow the same semantics as Cursors with regards
 * to read and write transactions and how they are closed.
 *
 * @param <T>
 */
public final class CursorIterator<T> implements
    Iterator<CursorIterator.KeyVal<T>>,
    AutoCloseable {

  private final Cursor<T> cursor;
  private KeyVal<T> entry;
  private boolean first = true;
  private final T key;
  private State state = NOT_READY;
  private final IteratorType type;

  CursorIterator(final Cursor<T> cursor, final T key, final IteratorType type) {
    this.cursor = cursor;
    this.type = type;
    this.key = key;
  }

  @Override
  public void close() {
    cursor.close();
  }

  @Override
  public boolean hasNext() {
    switch (state) {
      case DONE:
        return false;
      case READY:
        return true;
      default:
    }
    return tryToComputeNext();
  }

  /**
   *
   * @return
   */
  public Iterable<KeyVal<T>> iterable() {
    return () -> CursorIterator.this;
  }

  @Override
  public KeyVal<T> next() throws NoSuchElementException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = NOT_READY;
    KeyVal<T> result = entry;
    entry = null;
    return result;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void setEntry(final boolean success) {
    if (success) {
      this.entry = new KeyVal<>(cursor.key(), cursor.val());
    } else {
      this.entry = null;
    }
  }

  private boolean tryToComputeNext() {
    if (first) {
      if (key != null) {
        setEntry(cursor.get(key, MDB_SET_RANGE));
      } else if (type == FORWARD) {
        setEntry(cursor.first());
      } else {
        setEntry(cursor.last());
      }
      first = false;
      if (entry == null) {
        state = DONE;
        return false;
      }
    } else {
      if (type == FORWARD) {
        setEntry(cursor.next());
      } else {
        setEntry(cursor.prev());
      }
      if (entry == null) {
        state = DONE;
        return false;
      }
    }
    state = READY;
    return true;
  }

  /**
   * Holder for a key and value pair.
   *
   * @param <T> buffer type
   */
  public static final class KeyVal<T> {

    /**
     *
     */
    public final T key;

    /**
     *
     */
    public final T val;

    /**
     *
     * @param key
     * @param val
     */
    public KeyVal(T key, T val) {
      this.key = key;
      this.val = val;
    }
  }

  enum State {
    READY, NOT_READY, DONE, FAILED,
  }

  /**
   * Direction in terms of key ordering for CursorIterator.
   */
  enum IteratorType {
    FORWARD, BACKWARD
  }

}
