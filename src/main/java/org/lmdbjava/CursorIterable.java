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

import static org.lmdbjava.CursorIterable.State.*;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.lmdbjava.KeyRangeType.CursorOp;
import org.lmdbjava.KeyRangeType.IteratorOp;

/**
 * {@link Iterable} that creates a single {@link Iterator} that will iterate over a {@link Cursor}
 * as specified by a {@link KeyRange}.
 *
 * <p>An instance will create and close its own cursor.
 *
 * @param <T> buffer type
 */
public final class CursorIterable<T> implements Iterable<CursorIterable.KeyVal<T>>, AutoCloseable {

  private final Comparator<T> comparator;
  private final Cursor<T> cursor;
  private final KeyVal<T> entry;
  private boolean iteratorReturned;
  private final KeyRange<T> range;
  private State state = REQUIRES_INITIAL_OP;

  CursorIterable(
      final Txn<T> txn, final Dbi<T> dbi, final KeyRange<T> range, final Comparator<T> comparator) {
    this.cursor = dbi.openCursor(txn);
    this.range = range;
    this.comparator = comparator;
    this.entry = new KeyVal<>();
  }

  @Override
  public void close() {
    cursor.close();
  }

  /**
   * Obtain an iterator.
   *
   * <p>As iteration of the returned iterator will cause movement of the underlying LMDB cursor, an
   * {@link IllegalStateException} is thrown if an attempt is made to obtain the iterator more than
   * once. For advanced cursor control (such as being able to iterate over the same data multiple
   * times etc) please instead refer to {@link Dbi#openCursor(org.lmdbjava.Txn)}.
   *
   * @return an iterator
   */
  @Override
  public Iterator<KeyVal<T>> iterator() {
    if (iteratorReturned) {
      throw new IllegalStateException("Iterator can only be returned once");
    }
    iteratorReturned = true;

    return new Iterator<KeyVal<T>>() {
      @Override
      public boolean hasNext() {
        while (state != RELEASED && state != TERMINATED) {
          update();
        }
        return state == RELEASED;
      }

      @Override
      public KeyVal<T> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        state = REQUIRES_NEXT_OP;
        return entry;
      }

      @Override
      public void remove() {
        cursor.delete();
      }
    };
  }

  private void executeCursorOp(final CursorOp op) {
    boolean found;
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
      case GET_START_KEY_BACKWARD:
        found = cursor.get(range.getStart(), MDB_SET_RANGE);
        if (found) {
          if (!range.getType().isDirectionForward()
              && range.getType().isStartKeyRequired()
              && range.getType().isStartKeyInclusive()) {
            // We need to ensure we move to the last matching key if using DUPSORT, see issue 267
            boolean loop = true;
            while (loop) {
              if (comparator.compare(cursor.key(), range.getStart()) <= 0) {
                found = cursor.next();
                if (!found) {
                  // We got to the end so move last.
                  found = cursor.last();
                  loop = false;
                }
              } else {
                // We have moved past so go back one.
                found = cursor.prev();
                loop = false;
              }
            }
          }
        } else {
          found = cursor.last();
        }
        break;
      default:
        throw new IllegalStateException("Unknown cursor operation");
    }
    entry.setK(found ? cursor.key() : null);
    entry.setV(found ? cursor.val() : null);
  }

  private void executeIteratorOp() {
    final IteratorOp op =
        range.getType().iteratorOp(range.getStart(), range.getStop(), entry.key(), comparator);
    switch (op) {
      case CALL_NEXT_OP:
        executeCursorOp(range.getType().nextOp());
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
        executeCursorOp(range.getType().initialOp());
        state = REQUIRES_ITERATOR_OP;
        break;
      case REQUIRES_NEXT_OP:
        executeCursorOp(range.getType().nextOp());
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
   * <p>The same holder instance will always be returned for a given iterator. The returned keys and
   * values may change or point to different memory locations following changes in the iterator,
   * cursor or transaction.
   *
   * @param <T> buffer type
   */
  public static final class KeyVal<T> {

    private T k;
    private T v;

    /** Explicitly-defined default constructor to avoid warnings. */
    public KeyVal() {}

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

  /** Represents the internal {@link CursorIterable} state. */
  enum State {
    REQUIRES_INITIAL_OP,
    REQUIRES_NEXT_OP,
    REQUIRES_ITERATOR_OP,
    RELEASED,
    TERMINATED
  }
}
