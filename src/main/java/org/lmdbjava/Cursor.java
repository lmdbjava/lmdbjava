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

import java.nio.ByteBuffer;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.NativeLongByReference;
import static org.lmdbjava.CursorOp.MDB_SET;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.MDB_SUCCESS;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import org.lmdbjava.Txn.ReadWriteRequiredException;
import static org.lmdbjava.ValueBuffers.allocateMdbVal;
import static org.lmdbjava.ValueBuffers.setPointerToBuffer;

/**
 * A cursor handle.
 */
public class Cursor implements AutoCloseable {

  private boolean closed;
  private final Pointer k = allocateMdbVal();
  private final Pointer v = allocateMdbVal();
  private final long kAddress = k.address();
  private final long vAddress = v.address();
  private final Pointer ptr;
  private Txn tx;

  Cursor(final Pointer ptr, final Txn tx) throws BufferNotDirectException {
    this.ptr = ptr;
    this.tx = tx;
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
    if (SHOULD_CHECK && !tx.isReadOnly() && tx.isCommitted()) {
      throw new CommittedException();
    }
    LIB.mdb_cursor_close(ptr);
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
      tx.checkNotCommitted();
    }
    final NativeLongByReference longByReference = new NativeLongByReference();
    checkRc(LIB.mdb_cursor_count(ptr, longByReference));
    return longByReference.longValue();
  }

  /**
   * Delete current key/data pair.
   * <p>
   * This function deletes the key/data pair to which the cursor refers.
   *
   * @throws LmdbNativeException        if a native C error occurred
   * @throws CommittedException         if the transaction was committed
   * @throws ClosedException            if the cursor is already closed
   * @throws ReadWriteRequiredException if cursor using a read-only transaction
   */
  public void delete() throws LmdbNativeException, CommittedException,
                              ClosedException, ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      checkNotClosed();
      tx.checkNotCommitted();
      tx.checkWritesAllowed();
    }
    checkRc(LIB.mdb_cursor_del(ptr, 0));
  }

  /**
   * Reposition the read-only key/value buffers based on the passed key and
   * operation.
   * <p>
   * The method is only compatible with {@link CursorOp#MDB_SET},
   * {@link CursorOp#MDB_SET_KEY} and {@link CursorOp#MDB_SET_RANGE}. All
   * remaining operations can use the simpler and more efficient
   * {@link #position(org.lmdbjava.CursorOp)} method.
   *
   * @param key to find
   * @param op  options for this operation (see restrictions above)
   * @return false if key not found
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   * @throws CommittedException       if the transaction was committed
   * @throws ClosedException          if the cursor is already closed
   * @throws CursorOpException        if op code is invalid (see method docs)
   */
  public boolean get(final MdbVal key, MdbVal val, final CursorOp op)
      throws BufferNotDirectException, LmdbNativeException, CommittedException,
             ClosedException, CursorOpException {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(val);
      requireNonNull(op);
      checkNotClosed();
      tx.checkNotCommitted();
      if (op != MDB_SET && op != MDB_SET_KEY && op != MDB_SET_RANGE) {
        throw new CursorOpException();
      }
    }

    key.wrapInMdbValStruct(kAddress);

    final int rc = LIB.mdb_cursor_get(ptr, k, v, op.getCode());

    if (rc == MDB_SUCCESS) {
      key.wrapOutMdbValStruct(kAddress);
      val.wrapOutMdbValStruct(vAddress);
      return true;
    } else if (rc == MDB_NOTFOUND) {
      return false;
    } else {
      checkRc(rc);
      throw new IllegalStateException("Unreachable state");
    }
  }

  /**
   * Reposition the read-only key/value buffers based on the passed operation.
   * <p>
   * The method must NOT present {@link CursorOp#MDB_SET},
   * {@link CursorOp#MDB_SET_KEY} and {@link CursorOp#MDB_SET_RANGE}. Such
   * operations require a key, so you must instead use the
   * {@link #get(java.nio.ByteBuffer, org.lmdbjava.CursorOp)} method.
   *
   *
   * @param op options for this operation (see restrictions above)
   * @return false if key not found
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   * @throws CommittedException       if the transaction was committed
   * @throws ClosedException          if the cursor is already closed
   * @throws CursorOpException        if op code is invalid (see method docs)
   */
  public boolean position(MdbVal key, MdbVal val, CursorOp op)
      throws BufferNotDirectException, LmdbNativeException, CommittedException,
             ClosedException, CursorOpException {
    if (SHOULD_CHECK) {
      requireNonNull(op);
      checkNotClosed();
      tx.checkNotCommitted();
      if (op == MDB_SET || op == MDB_SET_KEY || op == MDB_SET_RANGE) {
        throw new CursorOpException();
      }
    }

    final int rc = LIB.mdb_cursor_get(ptr, k, v, op.getCode());

    if (rc == MDB_SUCCESS) {
      key.wrapOutMdbValStruct(kAddress);
      val.wrapOutMdbValStruct(vAddress);
      return true;
    }

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);

    throw new IllegalStateException("Unreachable state");
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
   * @throws BufferNotDirectException   if a passed buffer is invalid
   * @throws LmdbNativeException        if a native C error occurred
   * @throws CommittedException         if the transaction was committed
   * @throws ClosedException            if the cursor is already closed
   * @throws ReadWriteRequiredException if cursor using a read-only transaction
   */
  public void put(final ByteBuffer key, final ByteBuffer val,
                  final PutFlags... op)
      throws BufferNotDirectException, LmdbNativeException, CommittedException,
             ClosedException, ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(val);
      checkNotClosed();
      tx.checkNotCommitted();
      tx.checkWritesAllowed();
    }
    setPointerToBuffer(key, k);
    setPointerToBuffer(val, v);
    final int flags = mask(op);
    checkRc(LIB.mdb_cursor_put(ptr, k, v, flags));
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
      this.tx.checkReadOnly(); // existing
      tx.checkReadOnly(); // new
      tx.checkNotCommitted(); // new
    }
    this.tx = tx;
    checkRc(LIB.mdb_cursor_renew(tx.ptr, ptr));
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
   * CursorOp invalid for this method (check the documentation).
   */
  public static final class CursorOpException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public CursorOpException() {
      super("CursorOp invalid for this method");
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
