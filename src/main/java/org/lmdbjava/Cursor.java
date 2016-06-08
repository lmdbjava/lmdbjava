package org.lmdbjava;

import java.nio.ByteBuffer;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.NativeLongByReference;
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
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
  private final Transaction tx;

  Cursor(Pointer ptr, Transaction tx) {
    this.ptr = ptr;
    this.tx = tx;
  }

  /**
   * <p>
   *   Close a cursor handle.
   * </p>
   *
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
   * </p>
   *   Return count of duplicates for current key.
   * <p>
   * This call is only valid on databases that support sorted duplicate data
   * items {@link org.lmdbjava.DatabaseFlags#MDB_DUPSORT}.
   *
   * @return count of duplicates for current key
   * @throws LmdbNativeException if a native C error occurred
   */
  public long count() throws LmdbNativeException {
    NativeLongByReference longByReference = new NativeLongByReference();
    checkRc(lib.mdb_cursor_count(ptr, longByReference));
    return longByReference.longValue();
  }

  /**
   * Delete current key/data pair.
   * </p>
   * This function deletes the key/data pair to which the cursor refers.
   *
   * @throws LmdbNativeException if a native C error occurred
   */
  public void delete() throws LmdbNativeException {
    checkRc(lib.mdb_cursor_del(ptr, 0));
  }
  /**
   * <p>
   *   Retrieve by cursor.
   * </p>
   * This function retrieves key/data pairs from the database. The address and length
   * of the key are returned in the object to which \b key refers (except for the
   * case of the #MDB_SET option, in which the \b key object is unchanged), and
   * the address and length of the data are returned in the object to which \b data
   *
   * @param key Placeholder for the key memory address to be wrapped.
   * @param val Placeholder for the value memory address to be wrapped.
   * @param op A cursor operation.
   */
  public void get(ByteBuffer key, ByteBuffer val, CursorOp op)
      throws LmdbNativeException {
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
   * <p>
   *   Store by cursor.
   * </p>
   *
   * @param key The key operated on.
   * @param val The data operated on.
   *
   * @param op Options for this operation.
   */
  public void put(ByteBuffer key, ByteBuffer val, PutFlags... op)
      throws LmdbNativeException {
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
   * <p>
   *   Renew a cursor handle.
   * </p>
   *
   * A cursor is associated with a specific transaction and database.
   * Cursors that are only used in read-only
   * transactions may be re-used, to avoid unnecessary malloc/free overhead.
   * The cursor may be associated with a new read-only transaction, and
   * referencing the same database handle as it was created with.
   * This may be done whether the previous transaction is live or dead.
   *
   * @param tx transaction handle
   */
  public void renew(Transaction tx) throws LmdbNativeException {
    if (!tx.isReadOnly()) {
      throw new IllegalArgumentException("cannot renew write transactions");
    }
    checkRc(lib.mdb_cursor_renew(tx.ptr, ptr));
  }

}
