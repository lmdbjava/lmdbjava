package org.lmdbjava.core.lli;

import static jnr.ffi.LibraryLoader.create;
import jnr.ffi.Pointer;
import static jnr.ffi.Runtime.getRuntime;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.PointerByReference;

/**
 * JNF interface to LMDB.
 * <p>
 * For performance reasons pointers are used rather than structs.
 */
final class Library {

  static final Lmdb lib;
  static final jnr.ffi.Runtime runtime;

  static {
    lib = create(Lmdb.class).load("lmdb");
    runtime = getRuntime(lib);
  }


  private Library() {
  }
  public interface Lmdb {
    
    int mdb_env_create(PointerByReference envPtr);

    int mdb_env_set_maxreaders(@In Pointer env, int readers);

    int mdb_env_set_mapsize(@In Pointer env, long size);

    int mdb_env_set_maxdbs(@In Pointer env, int dbs);

    int mdb_env_open(@In Pointer env, @In String path, int flags, int mode);

    int mdb_txn_begin(@In Pointer env, @In Pointer parentTx, int flags,
                                                             Pointer txPtr);
    
    int mdb_dbi_open(@In Pointer txn, @In String name, int flags,
                                                       IntByReference dbiPtr);
    
    int mdb_txn_commit(@In Pointer ptr);

    int mdb_cursor_open(@In Pointer ptr, int dbi, PointerByReference cursorPtr);

    int mdb_cursor_get(@In Pointer cursor, Pointer k, @Out Pointer v,
                                                      int cursorOp);
  }
}
