package org.lmdbjava.core.lli;

import jnr.ffi.Pointer;
import jnr.ffi.Struct;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import org.lmdbjava.core.lli.Library.MDB_val;
import org.lmdbjava.core.lli.exceptions.LmdbNativeException;

import java.nio.ByteBuffer;

import static org.lmdbjava.core.lli.Library.lib;
import static org.lmdbjava.core.lli.Library.runtime;
import static org.lmdbjava.core.lli.exceptions.ResultCodeMapper.checkRc;


public class Cursor {
  private final Pointer ptr;
  private final boolean isReadOnly;
  private boolean closed;

  Cursor(Pointer ptr, boolean isReadOnly) {
    this.ptr = ptr;
    this.isReadOnly = isReadOnly;
  }

  public boolean isReadOnly() {
    return isReadOnly;
  }

  public void get(ByteBuffer key, ByteBuffer val, CursorOp op) throws LmdbNativeException {
    if (closed) {
      throw new IllegalArgumentException("Cursor closed");
    }
    final MDB_val k = new MDB_val(runtime);
    final MDB_val v = new MDB_val(runtime);

    checkRc(lib.mdb_cursor_get(ptr, k, v, op.getCode()));
    MemoryAccess.wrap(key, k.data.get().address(), (int) k.size.get());
    MemoryAccess.wrap(val, v.data.get().address(), (int) v.size.get());
  }

  public void seekKey(ByteBuffer key, ByteBuffer val) throws LmdbNativeException {
    if (closed) {
      throw new IllegalArgumentException("Cursor closed");
    }
    final MDB_val k = new MDB_val(runtime);
    k.size.set(key.limit());
    k.data.set(new ByteBufferMemoryIO(runtime, key));
    final MDB_val v = new MDB_val(runtime);

    checkRc(lib.mdb_cursor_get(ptr, k, v, CursorOp.MDB_SET_KEY.getCode()));

    MemoryAccess.wrap(key, k.data.get().address(), (int) k.size.get());
    MemoryAccess.wrap(val, v.data.get().address(), (int) v.size.get());
  }


  public void count() {

  }

  public void renew(Transaction tx) throws LmdbNativeException {
    checkRc(lib.mdb_cursor_renew(tx.ptr, ptr));
  }

  public void close() {
    if (!closed) {
      lib.mdb_cursor_close(ptr);
      closed = true;
    }
  }

}
