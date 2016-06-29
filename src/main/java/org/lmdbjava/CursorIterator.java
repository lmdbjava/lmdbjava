package org.lmdbjava;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for entries that follow the same semantics as Cursors
 * with regards to read and write transactions and how they are closed.
 * <p/>
 * <pre>
 * {@code
 * try (EntryIterator it = db.iterate()) {
 *   for (Entry next : it.iterable()) {
 *   }
 * }
 * }
 * </pre>
 */
public class CursorIterator<T> implements Iterator<CursorIterator.KeyVal<T>>, AutoCloseable {
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

  private KeyVal<T> holder = new KeyVal<>();
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
    return new Iterable<KeyVal<T>>() {
      @Override
      public Iterator<KeyVal<T>> iterator() {
        return CursorIterator.this;
      }
    };
  }

  @Override
  public void close() {
    cursor.close();
  }

  enum IteratorType {
    FORWARD, BACKWARD
  }

  public static class KeyVal<T> {
    T key;
    T val;
  }
}
