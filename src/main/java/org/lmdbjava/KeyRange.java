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

import static java.util.Objects.requireNonNull;
import static org.lmdbjava.KeyRangeType.BACKWARD_ALL;
import static org.lmdbjava.KeyRangeType.FORWARD_ALL;

/**
 * Limits the range and direction of keys to iterate.
 *
 * <p>
 * Immutable once created (although the buffers themselves may not be).
 *
 * @param <T> buffer type
 */
public final class KeyRange<T> {

  private static final KeyRange<?> BK = new KeyRange<>(BACKWARD_ALL, null, null);
  private static final KeyRange<?> FW = new KeyRange<>(FORWARD_ALL, null, null);
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
   * Create a {@link KeyRangeType#FORWARD_ALL} range.
   *
   * @param <T> buffer type
   * @return a key range (never null)
   */
  @SuppressWarnings({"checkstyle:SuppressWarnings", "unchecked"})
  public static <T> KeyRange<T> all() {
    return (KeyRange<T>) FW;
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_ALL} range.
   *
   * @param <T> buffer type
   * @return a key range (never null)
   */
  @SuppressWarnings({"checkstyle:SuppressWarnings", "unchecked"})
  public static <T> KeyRange<T> allBackward() {
    return (KeyRange<T>) BK;
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_AT_LEAST} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atLeast(final T start) {
    return new KeyRange<>(KeyRangeType.FORWARD_AT_LEAST, start, null);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_AT_LEAST} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atLeastBackward(final T start) {
    return new KeyRange<>(KeyRangeType.BACKWARD_AT_LEAST, start, null);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_AT_MOST} range.
   *
   * @param <T>  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atMost(final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_AT_MOST, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_AT_MOST} range.
   *
   * @param <T>  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atMostBackward(final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_AT_MOST, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_CLOSED} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closed(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_CLOSED, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_CLOSED} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closedBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_CLOSED, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_CLOSED_OPEN} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closedOpen(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_CLOSED_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_CLOSED_OPEN} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closedOpenBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_CLOSED_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_GREATER_THAN} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> greaterThan(final T start) {
    return new KeyRange<>(KeyRangeType.FORWARD_GREATER_THAN, start, null);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_GREATER_THAN} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> greaterThanBackward(final T start) {
    return new KeyRange<>(KeyRangeType.BACKWARD_GREATER_THAN, start, null);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_LESS_THAN} range.
   *
   * @param <T>  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> lessThan(final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_LESS_THAN, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_LESS_THAN} range.
   *
   * @param <T>  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> lessThanBackward(final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_LESS_THAN, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_OPEN} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> open(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_OPEN} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> openBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_OPEN_CLOSED} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> openClosed(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_OPEN_CLOSED, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_OPEN_CLOSED} range.
   *
   * @param <T>   buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> openClosedBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_OPEN_CLOSED, start, stop);
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

}
