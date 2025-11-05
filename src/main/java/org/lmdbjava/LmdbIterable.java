package org.lmdbjava;

import static org.lmdbjava.GetOp.MDB_SET_RANGE;

import java.util.Comparator;
import java.util.Iterator;
import org.lmdbjava.CursorIterable.KeyVal;

public class LmdbIterable<T> implements Iterable<KeyVal<T>>, AutoCloseable {

  private final Txn<T> txn;
  private final Cursor<T> cursor;
  private final Comparator<T> comparator;
  private final KeyRange<T> keyRange;
  private boolean iteratorReturned;

  private LmdbIterable(
      final Txn<T> txn,
      final Cursor<T> cursor,
      final Comparator<T> comparator,
      final KeyRange<T> keyRange) {
    this.txn = txn;
    this.cursor = cursor;
    this.comparator = comparator;
    this.keyRange = keyRange;
  }

  static <T> void iterate(final Txn<T> txn, final Dbi<T> dbi, final EntryConsumer<T> consumer) {
    try (final Cursor<T> cursor = dbi.openCursor(txn)) {
      boolean isFound = cursor.first();
      while (isFound) {
        consumer.accept(cursor.key(), cursor.val());
        isFound = cursor.next();
      }
    }
  }

  static <T> void iterate(
      final Txn<T> txn,
      final Dbi<T> dbi,
      final Comparator<T> comparator,
      final KeyRange<T> keyRange,
      final EntryConsumer<T> consumer) {
    try (final LmdbIterable<T> iterable = create(txn, dbi, comparator, keyRange)) {
      for (final KeyVal<T> entry : iterable) {
        consumer.accept(entry.key(), entry.val());
      }
    }
  }

  static <T> LmdbIterable<T> create(
      final Txn<T> txn, final Dbi<T> dbi, final Comparator<T> comparator) {
    return create(txn, dbi, comparator, KeyRange.all());
  }

  static <T> LmdbIterable<T> create(
      final Txn<T> txn,
      final Dbi<T> dbi,
      final Comparator<T> comparator,
      final KeyRange<T> keyRange) {
    final Cursor<T> cursor = dbi.openCursor(txn);
    try {
      return new LmdbIterable<>(txn, cursor, comparator, keyRange);
    } catch (final Error | RuntimeException e) {
      cursor.close();
      throw e;
    }
  }

  private static <T> LmdbIterator<T> createIterator(
      final Cursor<T> cursor,
      final BufferProxy<T> proxy,
      final Comparator<T> comparator,
      final KeyRange<T> keyRange) {
    final LmdbIterator<T> iterator;
    if (keyRange.getPrefix() != null) {
      if (keyRange.directionForward) {
        iterator = new LmdbPrefixIterator<>(cursor, proxy, keyRange.getPrefix());
      } else {
        iterator = new LmdbPrefixReversedIterator<>(cursor, proxy, keyRange.getPrefix());
      }
    } else if (keyRange.getStart() != null || keyRange.getStop() != null) {
      if (keyRange.directionForward) {
        iterator =
            new LmdbRangeIterator<>(
                cursor,
                comparator,
                keyRange.getStart(),
                keyRange.getStop(),
                keyRange.isStartKeyInclusive(),
                keyRange.isStopKeyInclusive());
      } else {
        iterator =
            new LmdbRangeReversedIterator<>(
                cursor,
                comparator,
                keyRange.getStart(),
                keyRange.getStop(),
                keyRange.isStartKeyInclusive(),
                keyRange.isStopKeyInclusive());
      }
    } else {
      if (keyRange.directionForward) {
        iterator = new LmdbIterator<>(cursor);
      } else {
        iterator = new LmdbReversedIterator<>(cursor);
      }
    }
    return iterator;
  }

  @Override
  public Iterator<KeyVal<T>> iterator() {
    if (iteratorReturned) {
      throw new IllegalStateException("Iterator can only be returned once");
    }
    iteratorReturned = true;
    return createIterator(cursor, txn.proxy, comparator, keyRange);
  }

  @Override
  public void close() {
    cursor.close();
  }

  public static class LmdbIterator<T> implements Iterator<KeyVal<T>> {

    final Cursor<T> cursor;
    Boolean isFound;
    final KeyVal<T> entry = new KeyVal<>();

    private LmdbIterator(final Cursor<T> cursor) {
      this.cursor = cursor;
    }

    @Override
    public boolean hasNext() {
      if (isFound == null) {
        isFound = cursor.first();
      } else {
        isFound = cursor.next();
      }
      return isFound;
    }

    @Override
    public KeyVal<T> next() {
      entry.setK(cursor.key());
      entry.setV(cursor.val());
      return entry;
    }

    @Override
    public void remove() {
      cursor.delete();
    }
  }

  private static class LmdbReversedIterator<T> extends LmdbIterator<T> {

    private LmdbReversedIterator(final Cursor<T> cursor) {
      super(cursor);
    }

    @Override
    public boolean hasNext() {
      if (isFound == null) {
        isFound = cursor.last();
      } else {
        isFound = cursor.prev();
      }
      return isFound;
    }
  }

  private static class LmdbRangeIterator<T> extends LmdbIterator<T> {

    private final Comparator<T> comparator;
    private final T start;
    private final T stop;
    private final boolean startInclusive;
    private final boolean stopInclusive;

    private LmdbRangeIterator(
        final Cursor<T> cursor,
        final Comparator<T> comparator,
        final T start,
        final T stop,
        final boolean startInclusive,
        final boolean stopInclusive) {
      super(cursor);
      this.comparator = comparator;
      this.start = start;
      this.stop = stop;
      this.startInclusive = startInclusive;
      this.stopInclusive = stopInclusive;
    }

    @Override
    public boolean hasNext() {
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
        final int compareResult = comparator.compare(stop, cursor.key());
        isFound = compareResult > 0 || (compareResult == 0 && stopInclusive);
      }

      return isFound;
    }
  }

  private static class LmdbRangeReversedIterator<T> extends LmdbIterator<T> {

    private final Comparator<T> comparator;
    private final T start;
    private final T stop;
    private final boolean startInclusive;
    private final boolean stopInclusive;

    private LmdbRangeReversedIterator(
        final Cursor<T> cursor,
        final Comparator<T> comparator,
        final T start,
        final T stop,
        final boolean startInclusive,
        final boolean stopInclusive) {
      super(cursor);
      this.comparator = comparator;
      this.start = start;
      this.stop = stop;
      this.startInclusive = startInclusive;
      this.stopInclusive = stopInclusive;
    }

    @Override
    public boolean hasNext() {
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
                if (comparator.compare(cursor.key(), start) <= 0) {
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
              final int compareResult = comparator.compare(start, cursor.key());
              if (compareResult < 0 || (compareResult == 0)) {
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
        final int compareResult = comparator.compare(stop, cursor.key());
        isFound = compareResult < 0 || (compareResult == 0 && stopInclusive);
      }

      return isFound;
    }
  }

  private static class LmdbPrefixIterator<T> extends LmdbIterator<T> {

    private final T prefix;
    private final BufferProxy<T> proxy;

    private LmdbPrefixIterator(final Cursor<T> cursor, final BufferProxy<T> proxy, final T prefix) {
      super(cursor);
      this.prefix = prefix;
      this.proxy = proxy;
    }

    @Override
    public boolean hasNext() {
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

  private static class LmdbPrefixReversedIterator<T> extends LmdbIterator<T> {

    private final BufferProxy<T> proxy;
    private final T prefix;
    private final T oneBigger;

    private LmdbPrefixReversedIterator(
        final Cursor<T> cursor, final BufferProxy<T> proxy, final T prefix) {
      super(cursor);
      this.prefix = prefix;
      this.proxy = proxy;

      // Create a prefix that is one bit greater than then supplied prefix.
      oneBigger = proxy.incrementLeastSignificantByte(prefix);
    }

    @Override
    public boolean hasNext() {
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
