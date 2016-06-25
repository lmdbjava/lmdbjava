/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.NativeLongByReference;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import org.lmdbjava.Txn.ReadWriteRequiredException;

/**
 * A cursor handle.
 *
 * @param <T> buffer type
 */
public final class Cursor<T> implements AutoCloseable {

  private boolean closed;
  private final Pointer ptrCursor;
  private final Txn<T> txn;

  Cursor(final Pointer ptr, final Txn<T> txn) {
    requireNonNull(ptr);
    requireNonNull(txn);
    this.ptrCursor = ptr;
    this.txn = txn;
  }

  /**
   * Close a cursor handle.
   * <p>
   * The cursor handle will be freed and must not be used again after this call.
   * Its transaction must still be live if it is a write-transaction.
   *
   * @throws CommittedException if the transaction was read-write and has
   *                            already been closed
   */
  @Override
  public void close() throws CommittedException {
    if (closed) {
      return;
    }
    if (SHOULD_CHECK && !txn.isReadOnly() && txn.isCommitted()) {
      throw new CommittedException();
    }
    LIB.mdb_cursor_close(ptrCursor);
    closed = true;
  }

  /**
   * Return count of duplicates for current key.
   * <p>
   * This call is only valid on databases that support sorted duplicate data
   * items {@link DbiFlags#MDB_DUPSORT}.
   *
   * @return count of duplicates for current key
   * @throws LmdbNativeException if a native C error occurred
   * @throws CommittedException  if the transaction was committed
   * @throws ClosedException     if the cursor is already closed
   */
  public long count() throws LmdbNativeException, CommittedException,
                             ClosedException {
    if (SHOULD_CHECK) {
      checkNotClosed();
      txn.checkNotCommitted();
    }
    final NativeLongByReference longByReference = new NativeLongByReference();
    checkRc(LIB.mdb_cursor_count(ptrCursor, longByReference));
    return longByReference.longValue();
  }

  /**
   * Delete current key/data pair.
   * <p>
   * This function deletes the key/data pair to which the cursor refers.
   *
   * @param f flags (either null or {@link PutFlags#MDB_NODUPDATA}
   * @throws LmdbNativeException        if a native C error occurred
   * @throws CommittedException         if the transaction was committed
   * @throws ClosedException            if the cursor is already closed
   * @throws ReadWriteRequiredException if cursor using a read-only transaction
   */
  public void delete(final PutFlags... f)
      throws LmdbNativeException, CommittedException, ClosedException,
             ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      checkNotClosed();
      txn.checkNotCommitted();
      txn.checkWritesAllowed();
    }
    final int flags = mask(f);
    checkRc(LIB.mdb_cursor_del(ptrCursor, flags));
  }

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key to search for
   * @param op  options for this operation
   * @return false if key not found
   * @throws LmdbNativeException if a native C error occurred
   * @throws CommittedException  if the transaction was committed
   * @throws ClosedException     if the cursor is already closed
   */
  public boolean get(final T key, final GetOp op)
      throws LmdbNativeException, CommittedException, ClosedException {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(op);
      checkNotClosed();
      txn.checkNotCommitted();
    }
    txn.keyIn(key);

    final int rc = LIB.mdb_cursor_get(ptrCursor, txn.ptrKey, txn.ptrVal,
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
   * Store by cursor.
   * <p>
   * This function stores key/data pairs into the database. The cursor is
   * positioned at the new item, or on failure usually near it.
   *
   * @param key key to store
   * @param val data to store
   * @param op  options for this operation
   * @throws LmdbNativeException        if a native C error occurred
   * @throws CommittedException         if the transaction was committed
   * @throws ClosedException            if the cursor is already closed
   * @throws ReadWriteRequiredException if cursor using a read-only transaction
   */
  public void put(final T key, final T val, final PutFlags... op)
      throws LmdbNativeException, CommittedException, ClosedException,
             ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(val);
      checkNotClosed();
      txn.checkNotCommitted();
      txn.checkWritesAllowed();
    }
    txn.keyIn(key);
    txn.valIn(val);
    final int flags = mask(op);
    checkRc(LIB.mdb_cursor_put(ptrCursor, txn.ptrKey, txn.ptrVal, flags));
    txn.keyOut();
    txn.valOut();
  }

  /**
   * Renew a cursor handle.
   * <p>
   * A cursor is associated with a specific transaction and database. Cursors
   * that are only used in read-only transactions may be re-used, to avoid
   * unnecessary malloc/free overhead. The cursor may be associated with a new
   * read-only transaction, and referencing the same database handle as it was
   * created with. This may be done whether the previous transaction is live or
   * dead.
   *
   * @param txn transaction handle
   * @throws LmdbNativeException       if a native C error occurred
   * @throws ClosedException           if the cursor is already closed
   * @throws CommittedException        if the new transaction was committed
   * @throws ReadOnlyRequiredException if a R-W transaction was presented
   */
  public void renew(final Txn<T> txn)
      throws LmdbNativeException, ClosedException, ReadOnlyRequiredException,
             CommittedException {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      checkNotClosed();
      this.txn.checkReadOnly(); // existing
      txn.checkReadOnly(); // new
      txn.checkNotCommitted(); // new
    }
    checkRc(LIB.mdb_cursor_renew(txn.ptr, ptrCursor));
  }

  /**
   * Reposition the key/value buffers based on the passed operation.
   *
   * @param op options for this operation
   * @return false if requested position not found
   * @throws LmdbNativeException if a native C error occurred
   * @throws CommittedException  if the transaction was committed
   * @throws ClosedException     if the cursor is already closed
   */
  public boolean seek(final SeekOp op)
      throws LmdbNativeException, CommittedException, ClosedException {
    if (SHOULD_CHECK) {
      requireNonNull(op);
      checkNotClosed();
      txn.checkNotCommitted();
    }

    final int rc = LIB.mdb_cursor_get(ptrCursor, txn.ptrKey, txn.ptrVal,
                                      op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    txn.keyOut();
    txn.valOut();
    return true;
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

    private static final long serialVersionUID = 1L;
    static final int MDB_CURSOR_FULL = -30_787;

    FullException() {
      super(MDB_CURSOR_FULL, "Cursor stack too deep - internal error");
    }
  }

}
