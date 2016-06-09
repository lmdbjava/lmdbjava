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
import static jnr.ffi.Memory.allocateDirect;
import static jnr.ffi.NativeType.ADDRESS;
import jnr.ffi.Pointer;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

/**
 * LMDB transaction.
 */
public final class Txn implements AutoCloseable {

  private boolean committed;
  private final boolean readOnly;
  private boolean reset = false;
  final Env env;
  final Txn parent;
  final Pointer ptr;

  /**
   * Create a transaction handle.
   * <p>
   * An transaction can be read-only or read-write, based on the passed
   * transaction flags.
   * <p>
   * The environment must be open at the time of calling this constructor.
   * <p>
   * A transaction must be finalised by calling {@link #commit()} or
   * {@link #abort()}.
   *
   * @param env    the owning environment (required)
   * @param parent parent transaction (may be null if no parent)
   * @param flags  applicable flags (eg for a reusable, read-only transaction)
   * @throws NotOpenException    if the environment is not currently open
   * @throws LmdbNativeException if a native C error occurred
   */
  public Txn(final Env env, final Txn parent,
             final TxnFlags... flags) throws NotOpenException,
                                             LmdbNativeException {
    requireNonNull(env);
    if (!env.isOpen() || env.isClosed()) {
      throw new NotOpenException();
    }
    this.env = env;
    final int flagsMask = mask(flags);
    this.readOnly = isSet(flagsMask, MDB_RDONLY);
    this.parent = parent;
    final Pointer txnPtr = allocateDirect(runtime, ADDRESS);
    final Pointer txnParentPtr = parent == null ? null : parent.ptr;
    checkRc(lib.mdb_txn_begin(env.ptr, txnParentPtr, flagsMask, txnPtr));
    ptr = txnPtr.getPointer(0);
  }

  /**
   * Create a write transaction handle without a parent transaction.
   *
   * @param env the owning environment (required)
   * @throws NotOpenException    if the environment is not currently open
   * @throws LmdbNativeException if a native C error occurred
   */
  public Txn(final Env env)
      throws NotOpenException, LmdbNativeException {
    this(env, null, (TxnFlags[]) null);
  }

  /**
   * Create a read or write transaction handle without a parent transaction.
   *
   * @param env   the owning environment (required)
   * @param flags applicable flags (eg for a reusable, read-only transaction)
   * @throws NotOpenException    if the environment is not currently open
   * @throws LmdbNativeException if a native C error occurred
   */
  public Txn(final Env env, TxnFlags... flags)
      throws NotOpenException, LmdbNativeException {
    this(env, null, flags);
  }

  /**
   * Aborts this transaction.
   *
   * @throws CommittedException if already committed
   */
  public void abort() throws CommittedException {
    if (committed) {
      throw new CommittedException();
    }
    lib.mdb_txn_abort(ptr);
    this.committed = true;
  }

  /**
   * Closes this transaction by aborting if not already committed
   */
  @Override
  public void close() {
    if (committed) {
      return;
    }
    lib.mdb_txn_abort(ptr);
    this.committed = true;
  }

  /**
   * Commits this transaction.
   *
   * @throws CommittedException  if already committed
   * @throws LmdbNativeException if a native C error occurred
   */
  public void commit() throws CommittedException, LmdbNativeException {
    if (committed) {
      throw new CommittedException();
    }
    checkRc(lib.mdb_txn_commit(ptr));
    this.committed = true;
  }

  /**
   * Return the transaction's ID.
   *
   * @return A transaction ID, valid if input is an active transaction
   */
  public long getId() {
    return lib.mdb_txn_id(ptr);
  }

  /**
   * Obtains this transaction's parent.
   *
   * @return the parent transaction (may be null)
   */
  public Txn getParent() {
    return parent;
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
   * Whether this transaction has been {@link #reset()}.
   *
   * @return if reset
   */
  public boolean isReset() {
    return reset;
  }

  /**
   * Renews a read-only transaction previously released by {@link #reset()}.
   *
   * @throws NotResetException   if reset not called
   * @throws LmdbNativeException if a native C error occurred
   */
  public void renew() throws NotResetException, LmdbNativeException {
    if (!reset) {
      throw new NotResetException();
    }
    reset = false;
    checkRc(lib.mdb_txn_renew(ptr));
  }

  /**
   * Aborts this read-only transaction and resets the transaction handle so it
   * can be reused upon calling {@link #renew()}.
   *
   * @throws ReadOnlyRequiredException if a read-write transaction
   * @throws ResetException            if reset already performed
   */
  public void reset() throws ReadOnlyRequiredException, ResetException {
    if (!isReadOnly()) {
      throw new ReadOnlyRequiredException();
    }
    if (reset) {
      throw new ResetException();
    }
    lib.mdb_txn_reset(ptr);
    reset = true;
  }

  /**
   * Transaction must abort, has a child, or is invalid.
   */
  public static final class BadException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_BAD_TXN = -30_782;

    BadException() {
      super(MDB_BAD_TXN, "Transaction must abort, has a child, or is invalid");
    }
  }

  /**
   * Invalid reuse of reader locktable slot.
   */
  public static final class BadReaderLockException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_BAD_RSLOT = -30_783;

    BadReaderLockException() {
      super(MDB_BAD_RSLOT, "Invalid reuse of reader locktable slot");
    }
  }

  /**
   * Transaction has already been committed.
   */
  public static final class CommittedException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public CommittedException() {
      super("Transaction has already been opened");
    }
  }

  /**
   * The current transaction has not been reset.
   */
  public static class NotResetException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public NotResetException() {
      super("Transaction has not been reset");
    }
  }

  /**
   * The current transaction is not a read-only transaction.
   */
  public static class ReadOnlyRequiredException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public ReadOnlyRequiredException() {
      super("Not a read-only transaction");
    }
  }

  /**
   * The current transaction has already been reset.
   */
  public static class ResetException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public ResetException() {
      super("Transaction has already been reset");
    }
  }

  /**
   * Transaction has too many dirty pages.
   */
  public static final class TxFullException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_TXN_FULL = -30_788;

    TxFullException() {
      super(MDB_TXN_FULL, "Transaction has too many dirty pages");
    }
  }

}
