/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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
 * @param <T> buffer type
 */
public final class CursorIterator<T> implements
    Iterator<CursorIterator.KeyVal<T>>,
    AutoCloseable {

  private final Cursor<T> cursor;
  private final KeyVal<T> entry;
  private boolean first = true;
  private final T key;
  private State state = NOT_READY;
  private final IteratorType type;

  CursorIterator(final Cursor<T> cursor, final T key, final IteratorType type) {
    this.cursor = cursor;
    this.type = type;
    this.key = key;
    this.entry = new KeyVal<>();
  }

  @Override
  public void close() {
    cursor.close();
  }

  @Override
  @SuppressWarnings("checkstyle:returncount")
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
   * Obtain an iterator.
   *
   * @return an iterator
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
    return entry;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void setEntry(final boolean success) {
    if (success) {
      this.entry.setKey(cursor.key());
      this.entry.setVal(cursor.val());
    } else {
      this.entry.setKey(null);
      this.entry.setVal(null);
    }
  }

  @SuppressWarnings("checkstyle:returncount")
  private boolean tryToComputeNext() {
    if (first) {
      if (key != null) { // NOPMD
        setEntry(cursor.get(key, MDB_SET_RANGE));
      } else if (type == FORWARD) {
        setEntry(cursor.first());
      } else {
        setEntry(cursor.last());
      }
      first = false;
      if (entry.isEmpty()) {
        state = DONE;
        return false;
      }
    } else {
      if (type == FORWARD) {
        setEntry(cursor.next());
      } else {
        setEntry(cursor.prev());
      }
      if (entry.isEmpty()) {
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
   * <p>
   * The same holder instance will always be returned for a given iterator.
   * The returned keys and values may change or point to different memory
   * locations following changes in the iterator, cursor or transaction.
   *
   * @param <T> buffer type
   */
  public static final class KeyVal<T> {

    private T k;
    private T v;

    /**
     * The key.
     *
     * @return key
     */
    public T key() {
      return k;
    }

    /**
     * The value.
     *
     * @return value
     */
    public T val() {
      return v;
    }

    void setK(final T key) {
      this.k = key;
    }

    void setV(final T val) {
      this.v = val;
    }

    boolean isEmpty() {
      return this.k == null && this.v == null;
  }

  /**
   * Direction in terms of key ordering for CursorIterator.
   */
  public enum IteratorType {
    /**
     * Move forward.
     */
    FORWARD,
    /**
     * Move backward.
     */
    BACKWARD
  }

  /**
   * Represents the internal {@link CursorIterator} state.
   */
  enum State {
    READY, NOT_READY, DONE, FAILED,
  }

}
