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

/**
 * Iterator for entries that follow the same semantics as Cursors
 * with regards to read and write transactions and how they are closed.
 */
public class CursorIterator<T> implements Iterator<KeyVal<T>>, AutoCloseable {
  private final Cursor<T> cursor;
  private final IteratorType type;
  private final T key;
  private State state = State.NOT_READY;

  CursorIterator(Cursor<T> cursor, T key, IteratorType type) {
    this.cursor = cursor;
    this.type = type;
    this.key = key;
  }

  private enum State {
    READY, NOT_READY, DONE, FAILED,
  }

  private KeyVal<T> holder = new KeyVal<>(null, null);
  private KeyVal<T> entry;
  private boolean first = true;

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

  private boolean tryToComputeNext() {
    if (first) {
      if (key != null) {
          setEntry(cursor.get(key, GetOp.MDB_SET_RANGE));
      } else {
        if (type == IteratorType.FORWARD) {
          setEntry(cursor.first());
        } else {
          setEntry(cursor.last());
        }
      }
      first = false;
      if (entry == null) {
        state = State.DONE;
        return false;
      }
    } else {
      if (type == IteratorType.FORWARD) {
        setEntry(cursor.next());
      } else {
        setEntry(cursor.prev());
      }
      if (entry == null) {
        state = State.DONE;
        return false;
      }
    }
    state = State.READY;
    return true;
  }

  private void setEntry(boolean success) {
    if (success) {
      holder.key = cursor.key();
      holder.val = cursor.val();
      this.entry = holder;
    } else {
      this.entry = null;
    }
  }

  @Override
  public KeyVal<T> next() throws NoSuchElementException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;
    KeyVal<T> result = entry;
    entry = null;
    return result;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public Iterable<KeyVal<T>> iterable() {
    return () -> CursorIterator.this;
  }

  @Override
  public void close() {
    cursor.close();
  }

  /**
   * Direction in terms of key ordering for CursorIterator.
   */
  enum IteratorType {
    FORWARD, BACKWARD
  }
}
