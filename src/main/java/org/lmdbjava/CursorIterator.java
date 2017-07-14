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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import static org.lmdbjava.CursorIterator.State.RELEASED;
import static org.lmdbjava.CursorIterator.State.REQUIRES_INITIAL_OP;
import static org.lmdbjava.CursorIterator.State.REQUIRES_ITERATOR_OP;
import static org.lmdbjava.CursorIterator.State.REQUIRES_NEXT_OP;
import static org.lmdbjava.CursorIterator.State.TERMINATED;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;
import org.lmdbjava.KeyRange.CursorOp;
import org.lmdbjava.KeyRange.IteratorOp;

/**
 * {@link Iterator} that iterates over a {@link Cursor} as specified by a
 * {@link KeyRange}.
 *
 * <p>
 * An instance will create and close its own cursor.
 *
 * @param <T> buffer type
 */
public final class CursorIterator<T> implements
    Iterator<CursorIterator.KeyVal<T>>, AutoCloseable {

  private final Comparator<T> comparator;
  private final Cursor<T> cursor;
  private final KeyVal<T> entry;
  private final KeyRange<T> range;
  private State state = REQUIRES_INITIAL_OP;

  CursorIterator(final Txn<T> txn, final Dbi<T> dbi, final KeyRange<T> range,
                 final Comparator<T> comparator) {
    this.cursor = dbi.openCursor(txn);
    this.range = range;
    this.comparator = comparator;
    this.entry = new KeyVal<>();
  }

  @Override
  public void close() {
    cursor.close();
  }

  @Override
  @SuppressWarnings("checkstyle:returncount")
  public boolean hasNext() {
    while (state != RELEASED && state != TERMINATED) {
      update();
    }
    return state == RELEASED;
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
    state = REQUIRES_NEXT_OP;
    return entry;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void executeCursorOp(final CursorOp op) {
    final boolean found;
    switch (op) {
      case FIRST:
        found = cursor.first();
        break;
      case LAST:
        found = cursor.last();
        break;
      case NEXT:
        found = cursor.next();
        break;
      case PREV:
        found = cursor.prev();
        break;
      case GET_START_KEY:
        found = cursor.get(range.getStart(), MDB_SET_RANGE);
        break;
      default:
        throw new IllegalStateException("Unknown cursor operation");
    }
    entry.setK(found ? cursor.key() : null);
    entry.setV(found ? cursor.val() : null);
  }

  private void executeIteratorOp() {
    final IteratorOp op = range.iteratorOp(comparator, entry.key());
    switch (op) {
      case CALL_NEXT_OP:
        executeCursorOp(range.nextOp());
        state = REQUIRES_ITERATOR_OP;
        break;
      case TERMINATE:
        state = TERMINATED;
        break;
      case RELEASE:
        state = RELEASED;
        break;
      default:
        throw new IllegalStateException("Unknown operation");
    }
  }

  private void update() {
    switch (state) {
      case REQUIRES_INITIAL_OP:
        executeCursorOp(range.initialOp());
        state = REQUIRES_ITERATOR_OP;
        break;
      case REQUIRES_NEXT_OP:
        executeCursorOp(range.nextOp());
        state = REQUIRES_ITERATOR_OP;
        break;
      case REQUIRES_ITERATOR_OP:
        executeIteratorOp();
        break;
      case TERMINATED:
        break;
      default:
        throw new IllegalStateException("Unknown state");
    }
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

  }

  /**
   * Direction in terms of key ordering for CursorIterator.
   *
   * @deprecated use {@link KeyRange} instead
   */
  @Deprecated
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
    REQUIRES_INITIAL_OP, REQUIRES_NEXT_OP, REQUIRES_ITERATOR_OP, RELEASED,
    TERMINATED
  }

}
