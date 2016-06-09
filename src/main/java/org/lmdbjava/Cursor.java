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
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.ValueBuffers.createVal;
import static org.lmdbjava.ValueBuffers.wrap;

/**
 * A cursor handle.
 */
public class Cursor {

  private ByteBuffer buffer;
  private boolean closed;
  private final Pointer ptr;
  private final Txn tx;

  Cursor(final Pointer ptr, final Txn tx) {
    this.ptr = ptr;
    this.tx = tx;
  }

  /**
   * Close a cursor handle.
   * <p>
   * The cursor handle will be freed and must not be used again after this call.
   * Its transaction must still be live if it is a write-transaction.
   */
  public void close() {
    if (!closed) {
      lib.mdb_cursor_close(ptr);
      closed = true;
    }
  }

  /**
   * Return count of duplicates for current key.
   * <p>
   * This call is only valid on databases that support sorted duplicate data
   * items {@link org.lmdbjava.DatabaseFlags#MDB_DUPSORT}.
   *
   * @return count of duplicates for current key
   * @throws LmdbNativeException if a native C error occurred
   */
  public long count() throws LmdbNativeException {
    final NativeLongByReference longByReference = new NativeLongByReference();
    checkRc(lib.mdb_cursor_count(ptr, longByReference));
    return longByReference.longValue();
  }

  /**
   * Delete current key/data pair.
   * <p>
   * This function deletes the key/data pair to which the cursor refers.
   *
   * @throws LmdbNativeException if a native C error occurred
   */
  public void delete() throws LmdbNativeException {
    checkRc(lib.mdb_cursor_del(ptr, 0));
  }

  /**
   * Retrieve by cursor.
   * <p>
   * This function retrieves key/data pairs from the database. The address and
   * length of the key are returned in the object to which \b key refers (except
   * for the case of the #MDB_SET option, in which the \b key object is
   * unchanged), and the address and length of the data are returned in the
   * object to which \b data
   *
   * @param key Placeholder for the key memory address to be wrapped.
   * @param val Placeholder for the value memory address to be wrapped.
   * @param op  A cursor operation.
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   */
  public void get(final ByteBuffer key, final ByteBuffer val, final CursorOp op)
      throws BufferNotDirectException, LmdbNativeException {
    requireNonNull(key);
    requireNonNull(val);
    requireNonNull(op);
    if (tx.isCommitted()) {
      throw new IllegalArgumentException("transaction already committed");
    }
    if (closed) {
      throw new IllegalArgumentException("Cursor closed");
    }
    final MDB_val k;
    final MDB_val v = new MDB_val(runtime);
    // set operations 15, 16, 17
    if (op.getCode() >= 15) {
      k = createVal(key);
    } else {
      k = new MDB_val(runtime);
    }
    checkRc(lib.mdb_cursor_get(ptr, k, v, op.getCode()));
    wrap(key, k);
    wrap(val, v);
  }

  /**
   * Store by cursor.
   * <p>
   *
   * @param key The key operated on.
   * @param val The data operated on.
   *
   * @param op  Options for this operation.
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   */
  public void put(final ByteBuffer key, final ByteBuffer val,
                  final PutFlags... op)
      throws BufferNotDirectException, LmdbNativeException {
    requireNonNull(key);
    requireNonNull(val);
    if (tx.isCommitted()) {
      throw new IllegalArgumentException("transaction already committed");
    }
    if (closed) {
      throw new IllegalArgumentException("cursor closed");
    }
    final MDB_val k = createVal(key);
    final MDB_val v = createVal(val);
    final int flags = mask(op);
    checkRc(lib.mdb_cursor_put(ptr, k, v, flags));
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
   * @throws LmdbNativeException if a native C error occurred
   */
  public void renew(final Txn tx) throws LmdbNativeException {
    if (!tx.isReadOnly()) {
      throw new IllegalArgumentException("cannot renew write transactions");
    }
    checkRc(lib.mdb_cursor_renew(tx.ptr, ptr));
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
