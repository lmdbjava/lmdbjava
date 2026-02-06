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

import static java.util.Objects.requireNonNull;
import static org.lmdbjava.KeyRangeType.BACKWARD_ALL;
import static org.lmdbjava.KeyRangeType.FORWARD_ALL;

import java.util.Objects;

/**
 * Limits the range and direction of keys to iterate.
 *
 * <p>Immutable once created (although the buffers themselves may not be).
 *
 * @param <T> buffer type
 */
public final class KeyRange<T> {

  private static final KeyRange<?> BK = new KeyRange<>(BACKWARD_ALL, null, null);
  private static final KeyRange<?> FW = new KeyRange<>(FORWARD_ALL, null, null);
  private final T start;
  private final T stop;
  private final T prefix;
  private final boolean startKeyInclusive;
  private final boolean stopKeyInclusive;
  final boolean directionForward;
  private KeyRangeType type;

  /**
   * Construct a key range.
   *
   * <p>End user code may find it more expressive to use one of the static methods provided on this
   * class.
   *
   * @param type key type
   * @param start start key (required if applicable for the passed range type)
   * @param stop stop key (required if applicable for the passed range type)
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
    this.prefix = null;
    this.startKeyInclusive = type.isStartKeyInclusive();
    this.stopKeyInclusive = type.isStopKeyInclusive();
    this.directionForward = type.isDirectionForward();
    this.type = type;
  }

  private KeyRange(
      final T start,
      final T stop,
      final boolean startKeyInclusive,
      final boolean stopKeyInclusive,
      final boolean directionForward) {
    this.start = start;
    this.stop = stop;
    this.prefix = null;
    this.startKeyInclusive = startKeyInclusive;
    this.stopKeyInclusive = stopKeyInclusive;
    this.directionForward = directionForward;
  }

  private KeyRange(final T prefix) {
    Objects.requireNonNull(prefix, "Prefix is required");
    this.start = null;
    this.stop = null;
    this.prefix = prefix;
    this.startKeyInclusive = false;
    this.stopKeyInclusive = false;
    this.directionForward = true;
  }

  private KeyRange(final T prefix, final boolean directionForward) {
    Objects.requireNonNull(prefix, "Prefix is required");
    this.start = null;
    this.stop = null;
    this.prefix = prefix;
    this.startKeyInclusive = false;
    this.stopKeyInclusive = false;
    this.directionForward = directionForward;
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_ALL} range.
   *
   * @param <T> buffer type
   * @return a key range (never null)
   */
  @SuppressWarnings("unchecked")
  public static <T> KeyRange<T> all() {
    return (KeyRange<T>) FW;
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_ALL} range.
   *
   * @param <T> buffer type
   * @return a key range (never null)
   */
  @SuppressWarnings("unchecked")
  public static <T> KeyRange<T> allBackward() {
    return (KeyRange<T>) BK;
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_AT_LEAST} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atLeast(final T start) {
    return new KeyRange<>(KeyRangeType.FORWARD_AT_LEAST, start, null);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_AT_LEAST} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atLeastBackward(final T start) {
    return new KeyRange<>(KeyRangeType.BACKWARD_AT_LEAST, start, null);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_AT_MOST} range.
   *
   * @param <T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atMost(final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_AT_MOST, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_AT_MOST} range.
   *
   * @param <T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> atMostBackward(final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_AT_MOST, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_CLOSED} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closed(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_CLOSED, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_CLOSED} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closedBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_CLOSED, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_CLOSED_OPEN} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closedOpen(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_CLOSED_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_CLOSED_OPEN} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> closedOpenBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_CLOSED_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_GREATER_THAN} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> greaterThan(final T start) {
    return new KeyRange<>(KeyRangeType.FORWARD_GREATER_THAN, start, null);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_GREATER_THAN} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> greaterThanBackward(final T start) {
    return new KeyRange<>(KeyRangeType.BACKWARD_GREATER_THAN, start, null);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_LESS_THAN} range.
   *
   * @param <T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> lessThan(final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_LESS_THAN, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_LESS_THAN} range.
   *
   * @param <T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> lessThanBackward(final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_LESS_THAN, null, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_OPEN} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> open(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_OPEN} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> openBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_OPEN, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#FORWARD_OPEN_CLOSED} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> openClosed(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.FORWARD_OPEN_CLOSED, start, stop);
  }

  /**
   * Create a {@link KeyRangeType#BACKWARD_OPEN_CLOSED} range.
   *
   * @param <T> buffer type
   * @param start start key (required)
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> openClosedBackward(final T start, final T stop) {
    return new KeyRange<>(KeyRangeType.BACKWARD_OPEN_CLOSED, start, stop);
  }

  /**
   * Create a prefix range.
   *
   * @param <T> buffer type
   * @param prefix key prefix (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> prefix(final T prefix) {
    return new KeyRange<>(prefix);
  }

  /**
   * Create a backward prefix range.
   *
   * @param <T> buffer type
   * @param prefix key prefix (required)
   * @return a key range (never null)
   */
  public static <T> KeyRange<T> prefixBackward(final T prefix) {
    return new KeyRange<>(prefix, false);
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
   * Is the start key to be treated as inclusive in the range.
   *
   * @return true if start key is inclusive. False if not inclusive or no start key is required by
   *     the range type.
   */
  public boolean isStartKeyInclusive() {
    return startKeyInclusive;
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
   * Key prefix.
   *
   * @return Key prefix (may be null)
   */
  public T getPrefix() {
    return prefix;
  }

  /**
   * Is the stop key to be treated as inclusive in the range.
   *
   * @return true if stop key is inclusive. False if not inclusive or no stop key is required by the
   *     range type.
   */
  public boolean isStopKeyInclusive() {
    return stopKeyInclusive;
  }

  /**
   * Whether the key space is iterated in the order provided by LMDB.
   *
   * @return true if forward, false if reverse
   */
  public boolean isDirectionForward() {
    return directionForward;
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
   * Create a new builder to construct a key range.
   *
   * @return A new key range builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private abstract static class BaseBuilder<T, B extends BaseBuilder<T, ?>> {

    boolean directionForward = true;

    private BaseBuilder() {}

    private BaseBuilder(final BaseBuilder<T, ?> builder) {
      this.directionForward = builder.directionForward;
    }

    public B reverse() {
      this.directionForward = false;
      return self();
    }

    public B reverse(final boolean reverse) {
      this.directionForward = !reverse;
      return self();
    }

    abstract B self();

    public abstract KeyRange<T> build();
  }

  public static class Builder {

    boolean directionForward = true;

    private Builder() {}

    public <T> PrefixBuilder<T> prefix(final T prefix) {
      Objects.requireNonNull(prefix, "Prefix is required");
      final PrefixBuilder<T> keyRange = new PrefixBuilder<>(directionForward);
      keyRange.prefix(prefix);
      return keyRange;
    }

    public <T> RangeBuilder<T> startInclusive(final T start) {
      return start(start, true);
    }

    public <T> RangeBuilder<T> startExclusive(final T start) {
      return start(start, false);
    }

    public <T> RangeBuilder<T> start(final T start, final boolean startInclusive) {
      Objects.requireNonNull(start, "Start is required");
      final RangeBuilder<T> range = new RangeBuilder<>(directionForward);
      return range.start(start, startInclusive);
    }

    public <T> RangeBuilder<T> stopInclusive(final T stop) {
      return stop(stop, true);
    }

    public <T> RangeBuilder<T> stopExclusive(final T stop) {
      return stop(stop, false);
    }

    public <T> RangeBuilder<T> stop(final T stop, final boolean stopInclusive) {
      Objects.requireNonNull(stop, "Stop is required");
      final RangeBuilder<T> range = new RangeBuilder<>(directionForward);
      return range.stop(stop, stopInclusive);
    }

    public <T> KeyRange<T> build() {
      return new KeyRange<>(null, null, false, false, directionForward);
    }
  }

  public static class PrefixBuilder<T> extends BaseBuilder<T, PrefixBuilder<T>> {

    T prefix;

    private PrefixBuilder(final boolean directionForward) {
      this.directionForward = directionForward;
    }

    private PrefixBuilder(final KeyRange<T> keyRange) {
      this.directionForward = keyRange.directionForward;
      this.prefix = keyRange.prefix;
    }

    public PrefixBuilder<T> prefix(final T prefix) {
      Objects.requireNonNull(prefix, "Prefix is required");
      this.prefix = prefix;
      return self();
    }

    @Override
    PrefixBuilder<T> self() {
      return this;
    }

    @Override
    public KeyRange<T> build() {
      return new KeyRange<>(prefix, directionForward);
    }
  }

  public static class RangeBuilder<T> extends BaseBuilder<T, RangeBuilder<T>> {

    T start;
    T stop;
    boolean startKeyInclusive;
    boolean stopKeyInclusive;

    private RangeBuilder(final boolean directionForward) {
      this.directionForward = directionForward;
    }

    private RangeBuilder(final KeyRange<T> keyRange) {
      this.directionForward = keyRange.directionForward;
      this.start = keyRange.start;
      this.stop = keyRange.stop;
      this.startKeyInclusive = keyRange.startKeyInclusive;
      this.stopKeyInclusive = keyRange.stopKeyInclusive;
    }

    public RangeBuilder<T> startInclusive(final T start) {
      return start(start, true);
    }

    public RangeBuilder<T> startExclusive(final T start) {
      return start(start, false);
    }

    public RangeBuilder<T> start(final T start, final boolean startKeyInclusive) {
      Objects.requireNonNull(start, "Start is required");
      this.start = start;
      this.startKeyInclusive = startKeyInclusive;
      return self();
    }

    public RangeBuilder<T> stopInclusive(final T stop) {
      return stop(stop, true);
    }

    public RangeBuilder<T> stopExclusive(final T stop) {
      return stop(stop, false);
    }

    public RangeBuilder<T> stop(final T stop, final boolean stopKeyInclusive) {
      Objects.requireNonNull(stop, "Stop is required");
      this.stop = stop;
      this.stopKeyInclusive = stopKeyInclusive;
      return self();
    }

    @Override
    RangeBuilder<T> self() {
      return this;
    }

    @Override
    public KeyRange<T> build() {
      return new KeyRange<>(start, stop, startKeyInclusive, stopKeyInclusive, directionForward);
    }
  }
}
