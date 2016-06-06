package org.lmdbjava.core.lli;

import static jnr.ffi.LibraryLoader.create;
import jnr.ffi.Pointer;
import static jnr.ffi.Runtime.getRuntime;

import jnr.ffi.Struct;
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

  public static final class MDB_val extends Struct {
    public final size_t size = new size_t();
    public final Pointer data = new Pointer();

    public MDB_val(jnr.ffi.Runtime runtime) {
      super(runtime);
    }
  }

  public interface Lmdb {

    Pointer mdb_version(int major, int minor, int patch);

    /**
     * Create an LMDB environment handle.
     */
    int mdb_env_create(PointerByReference envPtr);

    /**
     * 	Open an environment handle.
     */
    int mdb_env_open(@In Pointer env, @In String path, int flags, int mode);

    /**
     * Close the environment and release the memory map.
     */
    void mdb_env_close(@In Pointer env);

    /**
     * Set environment flags.
     */
    int mdb_env_set_flags(@In Pointer env, int flags, int onoff);

    /**
     * Get environment flags.
     */
    int mdb_env_get_flags(@In Pointer env, int flags);

    /**
     * Return the path that was used in mdb_env_open().
     */
    int mdb_env_get_path(@In Pointer env, String path);

    /**
     * Return the filedescriptor for the given environment.
     */
    int mdb_env_get_fd(@In Pointer env, @In Pointer fd);

    /**
     * Set the size of the memory map to use for this environment.
     */
    int mdb_env_set_mapsize(@In Pointer env, long size);

    /**
     * Set the maximum number of threads/reader slots for the environment.
     */
    int mdb_env_set_maxreaders(@In Pointer env, int readers);

    /**
     * Get the maximum number of threads/reader slots for the environment.
     */
    int mdb_env_get_maxreaders(@In Pointer env, int readers);

    /**
     * Set the maximum number of named databases for the environment.
     */
    int mdb_env_set_maxdbs(@In Pointer env, int dbs);

    /**
     * Get the maximum size of keys and MDB_DUPSORT data we can write.
     */
    int mdb_env_get_maxkeysize(@In Pointer env);


    /**
     * Set application information associated with the MDB_env.
     */
    // int mdb_env_set_userctx(@In Pointer env, void *ctx);


    /**
     * Get the application information associated with the MDB_env.
     */
    // void * mdb_env_get_userctx(@In Pointer env);


    /**
     * Set or reset the assert() callback of the environment
     */
    // int mdb_env_set_assert(@In Pointer env, MDB_assert_func *func)

    /**
     * Create a transaction for use with the environment.
     */
    int	mdb_txn_begin(@In Pointer env, @In Pointer parentTx, int flags, Pointer txPtr);


    /**
     * Returns the transaction's MDB_env.
     */
    Pointer mdb_txn_env(@In Pointer txn);


    /**
     * Return the transaction's ID.
     */
    long	mdb_txn_id(@In Pointer txn);


    /**
     * Commit all the operations of a transaction into the database.
     */
    int mdb_txn_commit(@In Pointer txn);


    /**
     * Abandon all the operations of the transaction instead of saving them.
     */
    void mdb_txn_abort(@In Pointer txn);


    /**
     * Reset a read-only transaction.
     */
    void mdb_txn_reset(@In Pointer txn);


    /**
     * Renew a read-only transaction.
     */
    int mdb_txn_renew(@In Pointer txn);


    /**
     * Open a database in the environment.
     */
    int mdb_dbi_open(@In Pointer txn, @In String name, int flags, IntByReference dbiPtr);

    /**
     * Retrieve statistics for a database.
     */
    // int mdb_stat(@In Pointer txn, int dbiPtr, MDB_stat *stat);


    /**
     * Retrieve the DB flags for a database handle.
     */
    int mdb_dbi_flags(@In Pointer txn, int dbiPtr, int flags);


    /**
     * Close a database handle. Normally unnecessary. Use with care:
     */
    void mdb_dbi_close(@In Pointer env, int dbiPtr);


    /**
     * Empty or delete+close a database.
     */
    int mdb_drop(@In Pointer txn, int dbiPtr, int del);


    /**
     * Set a custom key comparison function for a database.
     */
    // int mdb_set_compare(@In Pointer txn, int dbiPtr, MDB_cmp_func *cmp);


    /**
     * Set a custom data comparison function for a MDB_DUPSORT database.
     */
    // int mdb_set_dupsort(@In Pointer txn, int dbiPtr, MDB_cmp_func *cmp);


    /**
     * Set a relocation function for a MDB_FIXEDMAP database.
     */
    // int mdb_set_relfunc(@In Pointer txn, int dbiPtr, MDB_rel_func *rel);


    /**
     * Set a context pointer for a MDB_FIXEDMAP database's relocation function.
     */
    // int mdb_set_relctx(@In Pointer txn, int dbiPtr, void *ctx);


    /**
     * Get items from a database.
     */
    int mdb_get(@In Pointer txn, int dbiPtr, @In MDB_val key, @Out MDB_val data);


    /**
     * Store items into a database.
     */
    int mdb_put(@In Pointer txn, int dbiPtr, @In MDB_val key, @In MDB_val data, int flags);


    /**
     * Delete items from a database.
     */
    int mdb_del(@In Pointer txn, int dbiPtr, @In MDB_val key, @In MDB_val data);


    /**
     * Create a cursor handle.
     */
    int mdb_cursor_open(@In Pointer txn, int dbiPtr, PointerByReference cursorPtr);


    /**
     * Close a cursor handle.
     */
    void mdb_cursor_close(@In Pointer cursor);


    /**
     * Renew a cursor handle.
     */
    int mdb_cursor_renew(@In Pointer txn, @In Pointer cursor);


    /**
     * Return the cursor's transaction handle.
     */
    Pointer mdb_cursor_txn(@In Pointer cursor);


    /**
     * Return the cursor's database handle.
     */
    Pointer mdb_cursor_dbi(@In Pointer cursor);


    /**
     * Retrieve by cursor.
     */
    int mdb_cursor_get(@In Pointer cursor, @In MDB_val k, @Out MDB_val v, int cursorOp);

    /**
     * Store by cursor.
     */
    int mdb_cursor_put(@In Pointer cursor, @In MDB_val key, @In MDB_val data, int flags);

    /**
     * Delete current key/data pair.
     */
    int mdb_cursor_del(@In Pointer cursor, int flags);


    /**
     * Return count of duplicates for current key.
     */
    // int mdb_cursor_count(@In Pointer cursor, size_t *countp);


    /**
     * Compare two data items according to a particular database.
     */
    // int mdb_cmp(@In Pointer txn, int dbiPtr, const MDB_val *a, const MDB_val *b);


    /**
     * Compare two data items according to a particular database.
     */
    // int mdb_dcmp(@In Pointer txn, int dbiPtr, const MDB_val *a, const MDB_val *b);


    /**
     * Dump the entries in the reader lock table.
     */
    // int mdb_reader_list(@In Pointer env, MDB_msg_func *func, void *ctx);


    /**
     * Check for stale entries in the reader lock table.
     */
    int mdb_reader_check(@In Pointer env, int dead);

  }
}
