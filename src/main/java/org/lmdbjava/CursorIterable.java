/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
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
import static org.lmdbjava.CursorIterable.State.RELEASED;
import static org.lmdbjava.CursorIterable.State.REQUIRES_INITIAL_OP;
import static org.lmdbjava.CursorIterable.State.REQUIRES_ITERATOR_OP;
import static org.lmdbjava.CursorIterable.State.REQUIRES_NEXT_OP;
import static org.lmdbjava.CursorIterable.State.TERMINATED;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;
import org.lmdbjava.KeyRangeType.CursorOp;
import org.lmdbjava.KeyRangeType.IteratorOp;

/**
 * {@link Iterable} that creates a single {@link Iterator} that will iterate
 * over a {@link Cursor} as specified by a {@link KeyRange}.
 *
 * <p>
 * An instance will create and close its own cursor.
 *
 * <p>
 * If iterating over keys stored with {@link DbiFlags#MDB_INTEGERKEY} you must
 * provide a Java comparator when constructing the {@link Dbi} or this class. It
 * is more efficient to use a comparator only with this class, as this avoids
 * LMDB calling back into Java code to perform the integer key comparison.
 *
 * @param <T> buffer type
 */
public final class CursorIterable<T> implements
    Iterable<CursorIterable.KeyVal<T>>, AutoCloseable {

  private final Comparator<T> comparator;
  private final Cursor<T> cursor;
  private final KeyVal<T> entry;
  private boolean iteratorReturned;
  private final KeyRange<T> range;
  private State state = REQUIRES_INITIAL_OP;

  CursorIterable(final Txn<T> txn, final Dbi<T> dbi, final KeyRange<T> range,
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

  /**
   * Obtain an iterator.
   *
   * <p>
   * As iteration of the returned iterator will cause movement of the underlying
   * LMDB cursor, an {@link IllegalStateException} is thrown if an attempt is
   * made to obtain the iterator more than once. For advanced cursor control
   * (such as being able to iterate over the same data multiple times etc)
   * please instead refer to {@link Dbi#openCursor(org.lmdbjava.Txn)}.
   *
   * @return an iterator
   */
  @Override
  @SuppressWarnings("checkstyle:AnonInnerLength")
  public Iterator<KeyVal<T>> iterator() {
    if (iteratorReturned) {
      throw new IllegalStateException("Iterator can only be returned once");
    }
    iteratorReturned = true;

    return new Iterator<KeyVal<T>>() {
      @Override
      @SuppressWarnings("checkstyle:ReturnCount")
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

  @SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.NullAssignment"})
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
      case GET_START_KEY_BACKWARD:
        found = cursor.get(range.getStart(), MDB_SET_RANGE) || cursor.last();
        break;
      default:
        throw new IllegalStateException("Unknown cursor operation");
    }
    entry.setK(found ? cursor.key() : null);
    entry.setV(found ? cursor.val() : null);
  }

  private void executeIteratorOp() {
    final IteratorOp op = range.getType().iteratorOp(range.getStart(),
                                                     range.getStop(),
                                                     entry.key(), comparator);
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
   * Represents the internal {@link CursorIterable} state.
   */
  enum State {
    REQUIRES_INITIAL_OP, REQUIRES_NEXT_OP, REQUIRES_ITERATOR_OP, RELEASED,
    TERMINATED
  }

}
