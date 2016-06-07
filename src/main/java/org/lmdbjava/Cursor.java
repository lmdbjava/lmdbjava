package org.lmdbjava;

import java.nio.ByteBuffer;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
import static org.lmdbjava.MemoryAccess.createVal;
import static org.lmdbjava.MemoryAccess.wrap;
import static org.lmdbjava.PutFlags.ZERO;
import static org.lmdbjava.ResultCodeMapper.checkRc;

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

  public void count() {

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

  public void put(ByteBuffer key, ByteBuffer val) throws LmdbNativeException {
    put(key, val, ZERO);

  }

  public void put(ByteBuffer key, ByteBuffer val, PutFlags op)
      throws LmdbNativeException {
    requireNonNull(key);
    requireNonNull(val);
    requireNonNull(op);
    if (tx.isCommitted()) {
      throw new IllegalArgumentException("transaction already committed");
    }
    if (closed) {
      throw new IllegalArgumentException("cursor closed");
    }
    final MDB_val k = createVal(key);
    final MDB_val v = createVal(val);
    checkRc(lib.mdb_cursor_put(ptr, k, v, op.getMask()));
  }

  public void renew(Transaction tx) throws LmdbNativeException {
    checkRc(lib.mdb_cursor_renew(tx.ptr, ptr));
  }

}
