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
import jnr.ffi.provider.MemoryManager;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import static org.lmdbjava.CursorOp.MDB_SET;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
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

  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();

  private boolean closed;
  private final BufferProxy<T> proxy;
  private final Pointer ptrCursor;
  private final Pointer ptrKey;
  private final long ptrKeyAddr;
  private final Pointer ptrVal;
  private final long ptrValAddr;
  private final T roKey;
  private final T roVal;
  private Txn txn;

  Cursor(final Pointer ptr, final Txn tx, final BufferProxy<T> proxy) {
    requireNonNull(ptr);
    requireNonNull(tx);
    requireNonNull(proxy);
    this.ptrCursor = ptr;
    this.txn = tx;
    this.proxy = proxy;
    this.roKey = proxy.allocate(0);
    this.roVal = proxy.allocate(0);
    ptrKey = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrKeyAddr = ptrKey.address();
    ptrVal = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrValAddr = ptrVal.address();
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
  public void delete(PutFlags... f) throws LmdbNativeException,
                                           CommittedException,
                                           ClosedException,
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
   * @param key to search for (only needed for MDB_SET based ops)
   * @param op  options for this operation (see restrictions above)
   * @return false if key not found
   * @throws LmdbNativeException if a native C error occurred
   * @throws CommittedException  if the transaction was committed
   * @throws ClosedException     if the cursor is already closed
   */
  public boolean get(final T key, final CursorOp op)
      throws LmdbNativeException, CommittedException, ClosedException {
    if (SHOULD_CHECK) {
      requireNonNull(op);
      checkNotClosed();
      txn.checkNotCommitted();
    }

    if (op == MDB_SET || op == MDB_SET_KEY || op == MDB_SET_RANGE) {
      if (SHOULD_CHECK) {
        requireNonNull(key);
      }
      proxy.set(key, ptrKey, ptrKeyAddr);
    }

    final int rc = LIB.mdb_cursor_get(ptrCursor, ptrKey, ptrVal, op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    proxy.dirty(roKey, ptrKey, ptrKeyAddr);
    proxy.dirty(roVal, ptrVal, ptrValAddr);
    return true;
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory.
   * Any use of this buffer must comply with the standard LMDB C "mdb_get"
   * contract (ie do not modify, do not attempt to release the memory, do not
   * use once the transaction or cursor closes, do not use after a write etc).
   *
   * @return the key buffer (never null)
   */
  public T key() {
    return roKey;
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
  public void put(final T key, final T val,
                  final PutFlags... op)
      throws LmdbNativeException, CommittedException, ClosedException,
             ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(val);
      checkNotClosed();
      txn.checkNotCommitted();
      txn.checkWritesAllowed();
    }
    proxy.set(key, ptrKey, ptrKeyAddr);
    proxy.set(val, ptrVal, ptrValAddr);
    final int flags = mask(op);
    checkRc(LIB.mdb_cursor_put(ptrCursor, ptrKey, ptrVal, flags));
    proxy.dirty(roKey, ptrKey, ptrKeyAddr);
    proxy.dirty(roVal, ptrVal, ptrValAddr);
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
   * @param tx transaction handle
   * @throws LmdbNativeException       if a native C error occurred
   * @throws ClosedException           if the cursor is already closed
   * @throws CommittedException        if the new transaction was committed
   * @throws ReadOnlyRequiredException if a R-W transaction was presented
   */
  public void renew(final Txn tx)
      throws LmdbNativeException, ClosedException,
             ReadOnlyRequiredException,
             CommittedException {
    if (SHOULD_CHECK) {
      requireNonNull(tx);
      checkNotClosed();
      this.txn.checkReadOnly(); // existing
      tx.checkReadOnly(); // new
      tx.checkNotCommitted(); // new
    }
    this.txn = tx;
    checkRc(LIB.mdb_cursor_renew(tx.ptr, ptrCursor));
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory.
   * Any use of this buffer must comply with the standard LMDB C "mdb_get"
   * contract (ie do not modify, do not attempt to release the memory, do not
   * use once the transaction or cursor closes, do not use after a write etc).
   *
   * @return the value buffer (never null)
   */
  public T val() {
    return roVal;
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
