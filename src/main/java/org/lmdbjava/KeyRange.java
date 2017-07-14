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
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.KeyRange.CursorOp.FIRST;
import static org.lmdbjava.KeyRange.CursorOp.GET_START_KEY;
import static org.lmdbjava.KeyRange.CursorOp.LAST;
import static org.lmdbjava.KeyRange.CursorOp.NEXT;
import static org.lmdbjava.KeyRange.CursorOp.PREV;
import static org.lmdbjava.KeyRange.IteratorOp.CALL_NEXT_OP;
import static org.lmdbjava.KeyRange.IteratorOp.RELEASE;
import static org.lmdbjava.KeyRange.IteratorOp.TERMINATE;
import static org.lmdbjava.KeyRangeType.BACKWARD;
import static org.lmdbjava.KeyRangeType.FORWARD;

/**
 * Limits the range and direction of keys to iterate.
 *
 * <p>
 * Immutable once created (although the buffers themselves may not be).
 *
 * @param <T> buffer type
 */
@SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.StdCyclomaticComplexity"})
public final class KeyRange<T> {

  private static final KeyRange BACK = new KeyRange<>(BACKWARD, null, null);
  private static final KeyRange FORW = new KeyRange<>(FORWARD, null, null);
  private final T start;
  private final T stop;
  private final KeyRangeType type;

  /**
   * Construct a key range.
   *
   * <p>
   * End user code may find it more expressive to use one of the static methods
   * provided on this class.
   *
   * @param type  key type
   * @param start start key (required if applicable for the passed range type)
   * @param stop  stop key (required if applicable for the passed range type)
   */
  public KeyRange(final KeyRangeType type, final T start, final T stop) {
    requireNonNull(type, "Key range type is required");
    if (type.isStartKeyRequired()) {
      requireNonNull(start, "Start key is required for this key range type");
    }
    if (type.isStopKeyRequired()) {
      requireNonNull(stop, "Stop key is required for this key range type");
    }
    this.start = start;
    this.stop = stop;
    this.type = type;
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_START} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atLeast(final T start) {
    return new KeyRange<>(KeyRangeType.FORWARD_START, start, null);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_START} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atLeastBackward(final T start) {
    return new KeyRange<>(KeyRangeType.BACKWARD_START, start, null);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_STOP} range.
   *
   * @param <T>  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atMost(final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_STOP, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_STOP} range.
   *
   * @param <T>  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atMostBackward(final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_STOP, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD} range.
   *
   * @param <T> buffer type
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> backward() {
    return BACK;
  }

  /**
   * Create a {@link KeyRangeType#FORWARD} range.
   *
   * @param <T> buffer type
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> forward() {
    return FORW;
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_RANGE} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> range(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_RANGE, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_RANGE} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> rangeBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_RANGE, start, stop);
  }

  /**
   * Start key.
   *
   * @return start key (may be null)
   */
  public T getStart() {
    return start;
  }

  /**
   * Stop key.
   *
   * @return stop key (may be null)
   */
  public T getStop() {
    return stop;
  }

  /**
   * Key range type.
   *
   * @return type (never null)
   */
  public KeyRangeType getType() {
    return type;
  }

  /**
   * Determine the iterator action to take when iterator first begins.
   *
   * <p>
   * The iterator will perform this action and present the resulting key to
   * {@link #iteratorOp(java.util.Comparator, java.lang.Object)} for decision.
   *
   * @return appropriate action in response to this buffer
   */
  @SuppressWarnings("checkstyle:ReturnCount")
  CursorOp initialOp() {
    switch (type) {
      case FORWARD:
        return FIRST;
      case FORWARD_START:
        return GET_START_KEY;
      case FORWARD_STOP:
        return FIRST;
      case FORWARD_RANGE:
        return GET_START_KEY;
      case BACKWARD:
        return LAST;
      case BACKWARD_START:
        return GET_START_KEY;
      case BACKWARD_STOP:
        return LAST;
      case BACKWARD_RANGE:
        return GET_START_KEY;
      default:
        throw new IllegalStateException("Invalid type");
    }
  }

  /**
   * Determine the iterator's response to the presented key.
   *
   * @param <C>    comparator for the buffers
   * @param c      comparator (required)
   * @param buffer current key returned by LMDB (may be null)
   * @return response to this key
   */
  @SuppressWarnings("checkstyle:ReturnCount")
  <C extends Comparator<T>> IteratorOp iteratorOp(final C c,
                                                  final T buffer) {
    requireNonNull(c, "Comparator required");
    if (buffer == null) {
      return TERMINATE;
    }
    switch (type) {
      case FORWARD:
        return RELEASE;
      case FORWARD_START:
        return RELEASE;
      case FORWARD_STOP:
        return c.compare(buffer, stop) > 0 ? TERMINATE : RELEASE;
      case FORWARD_RANGE:
        return c.compare(buffer, stop) > 0 ? TERMINATE : RELEASE;
      case BACKWARD:
        return RELEASE;
      case BACKWARD_START:
        return c.compare(buffer, start) > 0 ? CALL_NEXT_OP : RELEASE; // rewind
      case BACKWARD_STOP:
        return c.compare(buffer, stop) >= 0 ? RELEASE : TERMINATE;
      case BACKWARD_RANGE:
        if (c.compare(buffer, start) > 0) {
          return CALL_NEXT_OP; // rewind
        }
        return c.compare(buffer, stop) >= 0 ? RELEASE : TERMINATE;
      default:
        throw new IllegalStateException("Invalid type");
    }
  }

  /**
   * Determine the iterator action to take when "next" is called or upon request
   * of {@link #iteratorOp(java.util.Comparator, java.lang.Object)}.
   *
   * <p>
   * The iterator will perform this action and present the resulting key to
   * {@link #iteratorOp(java.util.Comparator, java.lang.Object)} for decision.
   *
   * @return appropriate action for this key range type
   */
  CursorOp nextOp() {
    return type.isDirectionForward() ? NEXT : PREV;
  }

  /**
   * Action now required with the iterator.
   */
  enum IteratorOp {
    /**
     * Consider iterator completed.
     */
    TERMINATE,
    /**
     * Call {@link KeyRange#nextOp()} again and try again.
     */
    CALL_NEXT_OP,
    /**
     * Return the key to the user.
     */
    RELEASE
  }

  /**
   * Action now required with the cursor.
   */
  enum CursorOp {
    /**
     * Move to first.
     */
    FIRST,
    /**
     * Move to last.
     */
    LAST,
    /**
     * Get "start" key with {@link GetOp#MDB_SET_RANGE}.
     */
    GET_START_KEY,
    /**
     * Move forward.
     */
    NEXT,
    /**
     * Move backward.
     */
    PREV
  }

}
