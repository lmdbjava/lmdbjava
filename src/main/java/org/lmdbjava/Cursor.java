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

public class Cursor {

  private ByteBuffer buffer;
  private boolean closed;
  private final Pointer ptr;
  private final Transaction tx;

  Cursor(Pointer ptr, Transaction tx) {
    this.ptr = ptr;
    this.tx = tx;
  }

  public void close() {
    if (!closed) {
      lib.mdb_cursor_close(ptr);
      closed = true;
    }
  }

  /**
   * Return count of duplicates for current key.
   * </p>
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

  public void renew(Transaction tx) throws LmdbNativeException {
    if (!tx.isReadOnly()) {
      throw new IllegalArgumentException("cannot renew write transactions");
    }
    checkRc(lib.mdb_cursor_renew(tx.ptr, ptr));
  }

}
