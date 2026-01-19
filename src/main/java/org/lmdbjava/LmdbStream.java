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

import static org.lmdbjava.GetOp.MDB_SET_RANGE;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.lmdbjava.CursorIterable.KeyVal;

public class LmdbStream {

  public static <T> Stream<KeyVal<T>> stream(
      final Txn<T> txn, final Cursor<T> cursor, final RangeComparator rangeComparator) {
    return stream(txn, cursor, rangeComparator, KeyRange.all());
  }

  public static <T> Stream<KeyVal<T>> stream(
      final Txn<T> txn,
      final Cursor<T> cursor,
      final RangeComparator rangeComparator,
      final KeyRange<T> keyRange) {
    final LmdbSpliterator<T> spliterator =
        createSpliterator(cursor, rangeComparator, txn.proxy, keyRange);
    return StreamSupport.stream(spliterator, false).onClose(cursor::close);
  }

  static <T> LmdbSpliterator<T> createSpliterator(
      final Cursor<T> cursor,
      final RangeComparator rangeComparator,
      final BufferProxy<T> proxy,
      final KeyRange<T> keyRange) {
    final LmdbSpliterator<T> spliterator;
    if (keyRange.getPrefix() != null) {
      if (keyRange.directionForward) {
        spliterator =
            new LmdbPrefixSpliterator<>(cursor, rangeComparator, proxy, keyRange.getPrefix());
      } else {
        spliterator =
            new LmdbPrefixReversedSpliterator<>(
                cursor, rangeComparator, proxy, keyRange.getPrefix());
      }
    } else if (keyRange.getStart() != null || keyRange.getStop() != null) {
      if (keyRange.directionForward) {
        spliterator =
            new LmdbRangeSpliterator<>(
                cursor,
                rangeComparator,
                createEntryComparator(rangeComparator),
                keyRange.getStart(),
                keyRange.getStop(),
                keyRange.isStartKeyInclusive(),
                keyRange.isStopKeyInclusive());
      } else {
        spliterator =
            new LmdbRangeReversedSpliterator<>(
                cursor,
                rangeComparator,
                createReversedEntryComparator(rangeComparator),
                keyRange.getStart(),
                keyRange.getStop(),
                keyRange.isStartKeyInclusive(),
                keyRange.isStopKeyInclusive());
      }
    } else {
      if (keyRange.directionForward) {
        spliterator =
            new LmdbSpliterator<>(cursor, rangeComparator, createEntryComparator(rangeComparator));
      } else {
        spliterator =
            new LmdbReversedSpliterator<>(
                cursor, rangeComparator, createReversedEntryComparator(rangeComparator));
      }
    }
    return spliterator;
  }

  static class LmdbSpliterator<T> implements Spliterator<KeyVal<T>> {

    final Cursor<T> cursor;
    Boolean isFound;
    final KeyVal<T> entry = new KeyVal<>();
    final RangeComparator rangeComparator;
    final Comparator<KeyVal<T>> entryComparator;

    private LmdbSpliterator(
        final Cursor<T> cursor,
        final RangeComparator rangeComparator,
        final Comparator<KeyVal<T>> entryComparator) {
      this.cursor = cursor;
      this.rangeComparator = rangeComparator;
      this.entryComparator = entryComparator;
    }

    @Override
    public final boolean tryAdvance(final Consumer<? super KeyVal<T>> action) {
      if (hasNext()) {
        action.accept(createEntry());
        return true;
      }
      return false;
    }

    @Override
    public final void forEachRemaining(final Consumer<? super KeyVal<T>> action) {
      while (hasNext()) {
        action.accept(createEntry());
      }
    }

    boolean hasNext() {
      if (isFound == null) {
        isFound = cursor.first();
      } else {
        isFound = cursor.next();
      }
      return isFound;
    }

    private KeyVal<T> createEntry() {
      entry.setK(cursor.key());
      entry.setV(cursor.val());
      return entry;
    }

    @Override
    public final Spliterator<KeyVal<T>> trySplit() {
      // Splitting not allowed.
      return null;
    }

    @Override
    public final long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public final int characteristics() {
      return Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED | Spliterator.NONNULL;
    }

    @Override
    public Comparator<? super KeyVal<T>> getComparator() {
      return entryComparator;
    }
  }

  private static <T> Comparator<KeyVal<T>> createEntryComparator(
      final RangeComparator rangeComparator) {
    return null;
    //    return (o1, o2) -> comparator.compare(o1.key(), o2.key());
  }

  private static <T> Comparator<KeyVal<T>> createReversedEntryComparator(
      final RangeComparator rangeComparator) {
    return null;
    //    return (o1, o2) -> comparator.compare(o1.key(), o2.key());
  }

  private static class LmdbReversedSpliterator<T> extends LmdbSpliterator<T> {

    private LmdbReversedSpliterator(
        final Cursor<T> cursor,
        final RangeComparator rangeComparator,
        final Comparator<KeyVal<T>> entryComparator) {
      super(cursor, rangeComparator, entryComparator);
    }

    @Override
    boolean hasNext() {
      if (isFound == null) {
        isFound = cursor.last();
      } else {
        isFound = cursor.prev();
      }
      return isFound;
    }

    @Override
    public Comparator<? super KeyVal<T>> getComparator() {
      return entryComparator;
    }
  }

  private static class LmdbRangeSpliterator<T> extends LmdbSpliterator<T> {

    private final RangeComparator rangeComparator;
    private final T start;
    private final T stop;
    private final boolean startInclusive;
    private final boolean stopInclusive;

    private LmdbRangeSpliterator(
        final Cursor<T> cursor,
        final RangeComparator rangeComparator,
        final Comparator<KeyVal<T>> entryComparator,
        final T start,
        final T stop,
        final boolean startInclusive,
        final boolean stopInclusive) {
      super(cursor, rangeComparator, entryComparator);
      this.rangeComparator = rangeComparator;
      this.start = start;
      this.stop = stop;
      this.startInclusive = startInclusive;
      this.stopInclusive = stopInclusive;
    }

    @Override
    boolean hasNext() {
      if (isFound == null) {
        if (start == null) {
          isFound = cursor.first();
        } else {
          isFound = cursor.get(start, GetOp.MDB_SET_RANGE);
          if (isFound && !startInclusive) {
            while (isFound && start.equals(cursor.key())) {
              // Loop until we move past the start key. Looping in case of duplicate keys using
              // DUPSORT.
              // TODO: We could use increment LSB instead.
              isFound = cursor.next();
            }
          }
        }
      } else {
        isFound = cursor.next();
      }

      if (isFound && stop != null) {
        final int compareResult = rangeComparator.compareToStopKey();
        isFound = compareResult < 0 || (compareResult == 0 && stopInclusive);
      }

      return isFound;
    }
  }

  private static class LmdbRangeReversedSpliterator<T> extends LmdbReversedSpliterator<T> {

    private final RangeComparator rangeComparator;
    private final T start;
    private final T stop;
    private final boolean startInclusive;
    private final boolean stopInclusive;

    private LmdbRangeReversedSpliterator(
        final Cursor<T> cursor,
        final RangeComparator rangeComparator,
        final Comparator<KeyVal<T>> entryComparator,
        final T start,
        final T stop,
        final boolean startInclusive,
        final boolean stopInclusive) {
      super(cursor, rangeComparator, entryComparator);
      this.rangeComparator = rangeComparator;
      this.start = start;
      this.stop = stop;
      this.startInclusive = startInclusive;
      this.stopInclusive = stopInclusive;
    }

    @Override
    boolean hasNext() {
      if (isFound == null) {
        if (start == null) {
          isFound = cursor.last();
        } else {
          isFound = cursor.get(start, MDB_SET_RANGE);
          if (isFound) {
            if (startInclusive) {
              // TODO : We could increment LSB here and move backward.
              // We need to ensure we move to the last matching key if using DUPSORT, see issue 267
              boolean loop = true;
              while (loop) {
                if (rangeComparator.compareToStartKey() <= 0) {
                  isFound = cursor.next();
                  if (!isFound) {
                    // We got to the end so move last.
                    isFound = cursor.last();
                    loop = false;
                  }
                } else {
                  // We have moved past so go back one.
                  isFound = cursor.prev();
                  loop = false;
                }
              }
            } else {
              final int compareResult = rangeComparator.compareToStartKey();
              if (compareResult > 0 || (compareResult == 0)) {
                isFound = cursor.prev();
              }
            }
          } else {
            isFound = cursor.last();
          }
        }
      } else {
        isFound = cursor.prev();
      }

      if (isFound && stop != null) {
        final int compareResult = rangeComparator.compareToStopKey();
        isFound = compareResult > 0 || (compareResult == 0 && stopInclusive);
      }

      return isFound;
    }
  }

  private static class LmdbPrefixSpliterator<T> extends LmdbSpliterator<T> {

    private final BufferProxy<T> proxy;
    private final T prefix;

    private LmdbPrefixSpliterator(
        final Cursor<T> cursor,
        final RangeComparator rangeComparator,
        final BufferProxy<T> proxy,
        final T prefix) {
      super(cursor, rangeComparator, createEntryComparator(rangeComparator));
      this.proxy = proxy;
      this.prefix = prefix;
    }

    @Override
    boolean hasNext() {
      if (isFound == null) {
        isFound = cursor.get(prefix, GetOp.MDB_SET_RANGE);
      } else {
        isFound = cursor.next();
      }

      if (isFound) {
        isFound = proxy.containsPrefix(cursor.key(), prefix);
      }

      return isFound;
    }
  }

  private static class LmdbPrefixReversedSpliterator<T> extends LmdbReversedSpliterator<T> {

    private final BufferProxy<T> proxy;
    private final T prefix;
    private final T oneBigger;

    private LmdbPrefixReversedSpliterator(
        final Cursor<T> cursor,
        final RangeComparator rangeComparator,
        final BufferProxy<T> proxy,
        final T prefix) {
      super(cursor, rangeComparator, createReversedEntryComparator(rangeComparator));
      this.proxy = proxy;
      this.prefix = prefix;

      // Create a prefix that is one bit greater than then supplied prefix.
      oneBigger = proxy.incrementLeastSignificantByte(prefix);
    }

    @Override
    boolean hasNext() {
      if (isFound == null) {
        // If we don't have a byte buffer that is one bigger than the prefix then go to the last
        // row.
        if (oneBigger == null) {
          isFound = cursor.last();
        } else {
          // We have a byte buffer that is one bigger than the prefix so navigate to that row or the
          // next
          // biggest if no exact match.
          isFound = cursor.get(oneBigger, GetOp.MDB_SET_RANGE);
          if (isFound) {
            // If we found a row then move to the previous row.
            isFound = cursor.prev();
          } else {
            // We didn't find a row so go to the last row.
            isFound = cursor.last();
          }
        }
      } else {
        isFound = cursor.prev();
      }

      if (!isFound) {
        return false;
      }

      return proxy.containsPrefix(cursor.key(), prefix);
    }
  }
}
