/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
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
import jnr.ffi.Pointer;
import jnr.ffi.byref.NativeLongByReference;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.PutFlags.MDB_RESERVE;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_NEXT;
import static org.lmdbjava.SeekOp.MDB_PREV;

/**
 * A cursor handle.
 *
 * @param <T> buffer type
 */
public final class Cursor<T> implements AutoCloseable {

  private boolean closed;
  private final Pointer ptrCursor;
  private Txn<T> txn;

  Cursor(final Pointer ptr, final Txn<T> txn) {
    requireNonNull(ptr);
    requireNonNull(txn);
    this.ptrCursor = ptr;
    this.txn = txn;
  }

  /**
   * Close a cursor handle.
   *
   * <p>
   * The cursor handle will be freed and must not be used again after this call.
   * Its transaction must still be live if it is a write-transaction.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    if (SHOULD_CHECK && !txn.isReadOnly()) {
      txn.checkReady();
    }
    LIB.mdb_cursor_close(ptrCursor);
    closed = true;
  }

  /**
   * Return count of duplicates for current key.
   *
   * <p>
   * This call is only valid on databases that support sorted duplicate data
   * items {@link DbiFlags#MDB_DUPSORT}.
   *
   * @return count of duplicates for current key
   */
  public long count() {
    if (SHOULD_CHECK) {
      checkNotClosed();
      txn.checkReady();
    }
    final NativeLongByReference longByReference = new NativeLongByReference();
    checkRc(LIB.mdb_cursor_count(ptrCursor, longByReference));
    return longByReference.longValue();
  }

  /**
   * Delete current key/data pair.
   *
   * <p>
   * This function deletes the key/data pair to which the cursor refers.
   *
   * @param f flags (either null or {@link PutFlags#MDB_NODUPDATA}
   */
  public void delete(final PutFlags... f) {
    if (SHOULD_CHECK) {
      checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    final int flags = mask(f);
    checkRc(LIB.mdb_cursor_del(ptrCursor, flags));
  }

  /**
   * Position at first key/data item.
   *
   * @return false if requested position not found
   */
  public boolean first() {
    return seek(MDB_FIRST);
  }

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key to search for
   * @param op  options for this operation
   * @return false if key not found
   */
  public boolean get(final T key, final GetOp op) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(op);
      checkNotClosed();
      txn.checkReady();
    }
    txn.keyIn(key);

    final int rc = LIB.mdb_cursor_get(ptrCursor, txn.pointerKey(), txn.
                                      pointerVal(), op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    txn.keyOut();
    txn.valOut();
    return true;
  }

  /**
   * @return the key that the cursor is located at.
   */
  public T key() {
    return txn.key();
  }

  /**
   * Position at last key/data item.
   *
   * @return false if requested position not found
   */
  public boolean last() {
    return seek(MDB_LAST);
  }

  /**
   * Position at next data item.
   *
   * @return false if requested position not found
   */
  public boolean next() {
    return seek(MDB_NEXT);
  }

  /**
   * Position at previous data item.
   *
   * @return false if requested position not found
   */
  public boolean prev() {
    return seek(MDB_PREV);
  }

  /**
   * Store by cursor.
   *
   * <p>
   * This function stores key/data pairs into the database. The cursor is
   * positioned at the new item, or on failure usually near it.
   *
   * @param key key to store
   * @param val data to store
   * @param op  options for this operation
   */
  public void put(final T key, final T val, final PutFlags... op) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(val);
      checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    txn.keyIn(key);
    txn.valIn(val);
    final int flags = mask(op);
    checkRc(LIB.mdb_cursor_put(ptrCursor, txn.pointerKey(), txn.pointerVal(),
                               flags));
    txn.keyOut();
    txn.valOut();
  }

  /**
   * Renew a cursor handle.
   *
   * <p>
   * A cursor is associated with a specific transaction and database. Cursors
   * that are only used in read-only transactions may be re-used, to avoid
   * unnecessary malloc/free overhead. The cursor may be associated with a new
   * read-only transaction, and referencing the same database handle as it was
   * created with. This may be done whether the previous transaction is live or
   * dead.
   *
   * @param newTxn transaction handle
   */
  public void renew(final Txn<T> newTxn) {
    if (SHOULD_CHECK) {
      requireNonNull(newTxn);
      checkNotClosed();
      this.txn.checkReadOnly(); // existing
      newTxn.checkReadOnly();
      newTxn.checkReady();
    }
    checkRc(LIB.mdb_cursor_renew(newTxn.pointer(), ptrCursor));
    this.txn = newTxn;
  }

  /**
   * Reserve space for data of the given size, but don't copy the given val.
   * Instead, return a pointer to the reserved space, which the caller can fill
   * in later - before the next update operation or the transaction ends. This
   * saves an extra memcpy if the data is being generated later. LMDB does
   * nothing else with this memory, the caller is expected to modify all of the
   * space requested.
   *
   * <p>
   * This flag must not be specified if the database was opened with MDB_DUPSORT
   *
   * @param key  key to store in the database (not null)
   * @param size size of the value to be stored in the database (not null)
   * @return a buffer that can be used to modify the value
   */
  public T reserve(final T key, final int size) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    txn.keyIn(key);
    txn.valIn(size);
    final int flags = mask(MDB_RESERVE);
    checkRc(LIB.mdb_cursor_put(ptrCursor, txn.pointerKey(), txn.pointerVal(),
                               flags));
    txn.valOut();
    return txn.val();
  }

  /**
   * Reposition the key/value buffers based on the passed operation.
   *
   * @param op options for this operation
   * @return false if requested position not found
   */
  public boolean seek(final SeekOp op) {
    if (SHOULD_CHECK) {
      requireNonNull(op);
      checkNotClosed();
      txn.checkReady();
    }

    final int rc = LIB.mdb_cursor_get(ptrCursor, txn.pointerKey(), txn.
                                      pointerVal(),
                                      op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    txn.keyOut();
    txn.valOut();
    return true;
  }

  /**
   * @return the value that the cursor is located at.
   */
  public T val() {
    return txn.val();
  }

  private void checkNotClosed() throws ClosedException {
    if (closed) {
      throw new ClosedException();
    }
  }

  /**
   * Cursor has already been closed.
   */
  public static final class ClosedException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public ClosedException() {
      super("Cursor has already been closed");
    }
  }

  /**
   * Cursor stack too deep - internal error.
   */
  public static final class FullException extends LmdbNativeException {

    static final int MDB_CURSOR_FULL = -30_787;
    private static final long serialVersionUID = 1L;

    FullException() {
      super(MDB_CURSOR_FULL, "Cursor stack too deep - internal error");
    }
  }

}
