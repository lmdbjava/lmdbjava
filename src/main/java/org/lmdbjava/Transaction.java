package org.lmdbjava;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.util.Set;

import static jnr.ffi.Memory.allocateDirect;
import static jnr.ffi.NativeType.ADDRESS;

import jnr.ffi.Pointer;

import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.TransactionFlags.MDB_RDONLY;

/**
 * LMDB transaction.
 */
public final class Transaction implements Closeable {

  private boolean committed;
  private final Env env;
  private final boolean readOnly;
  final Pointer ptr;

  Transaction(final Env env, final Transaction parent,
              final TransactionFlags... flags) throws NotOpenException,
    LmdbNativeException {
    requireNonNull(env);
    if (!env.isOpen()) {
      throw new NotOpenException(Transaction.class.getSimpleName());
    }
    this.env = env;
    final int flagsMask = mask(flags);
    this.readOnly = isSet(flagsMask, MDB_RDONLY);
    final Pointer txnPtr = allocateDirect(runtime, ADDRESS);
    final Pointer txnParentPtr = parent == null ? null : parent.ptr;
    checkRc(lib.mdb_txn_begin(env.ptr, txnParentPtr, flagsMask, txnPtr));
    ptr = txnPtr.getPointer(0);
  }

  /**
   * Aborts this transaction.
   *
   * @throws AlreadyCommittedException if already committed
   * @throws LmdbNativeException       if a native C error occurred
   */
  public void abort() throws AlreadyCommittedException {
    if (committed) {
      throw new AlreadyCommittedException();
    }
    lib.mdb_txn_abort(ptr);
    this.committed = true;
  }

  /**
   * Commits this transaction.
   *
   * @throws AlreadyCommittedException if already committed
   * @throws LmdbNativeException       if a native C error occurred
   */
  public void commit() throws AlreadyCommittedException, LmdbNativeException {
    if (committed) {
      throw new AlreadyCommittedException();
    }
    checkRc(lib.mdb_txn_commit(ptr));
    this.committed = true;
  }

  /**
   * Opens a new database
   *
   * @param name  the name of the database (required)
   * @param flags applicable flags (required)
   * @return the database (never null)
   * @throws AlreadyCommittedException if already committed
   * @throws LmdbNativeException       if a native C error occurred
   */
  public Database databaseOpen(final String name, final DatabaseFlags... flags)
    throws AlreadyCommittedException, LmdbNativeException {
    return new Database(env, this, name, flags);
  }

  /**
   * Whether this transaction has been committed.
   *
   * @return true if committed
   */
  public boolean isCommitted() {
    return committed;
  }

  /**
   * Whether this transaction is read-only.
   *
   * @return if read-only
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * <p>
   * Return the transaction's ID.
   * </p>
   *
   * @return A transaction ID, valid if input is an active transaction.
   */
  public long getId() {
    return lib.mdb_txn_id(ptr);
  }

  @Override
  public void close() {
    if (!isCommitted()) {
      try {
        abort();
      } catch (AlreadyCommittedException e) {
      }
    }
  }
}
