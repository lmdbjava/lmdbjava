/*
 * Copyright © 2016-2026 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static jnr.ffi.Memory.allocateDirect;
import static jnr.ffi.NativeType.ADDRESS;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.Txn.State.DONE;
import static org.lmdbjava.Txn.State.READY;
import static org.lmdbjava.Txn.State.RELEASED;
import static org.lmdbjava.Txn.State.RESET;
import static org.lmdbjava.TxnFlags.MDB_RDONLY_TXN;

import java.util.Objects;
import jnr.ffi.Pointer;

/**
 * An LMDB ACID transaction.
 *
 * <p>A transaction belongs to an {@link Env} and must be closed before the {@link Env} is closed.
 * Only one concurrent write transaction is supported. Attempts to open another write transaction
 * will block until the open write transaction is closed.
 *
 * <p>{@link Txn#commit()} must be called to commit any changes made within the transaction.
 *
 * <p>Uncommitted changes can be rolled back by either calling {@link Txn#close()} or calling {@link
 * Txn#abort()}.
 *
 * <p>Closing a transaction without first calling {@link Txn#commit()} will perform an implicit
 * rollback of any uncommitted changes made within the transaction.
 *
 * <p>Transactions can be nested
 *
 * @param <T> buffer type
 */
public final class Txn<T> implements AutoCloseable {

  private final KeyVal<T> keyVal;
  private final Txn<T> parent;
  private final BufferProxy<T> proxy;
  private final Pointer ptr;
  private final boolean readOnly;
  private final Env<T> env;
  private final RefCounter.RefCounterReleaser refCounterReleaser;
  private volatile State state;

  Txn(final Env<T> env, final Txn<T> parent, final BufferProxy<T> proxy, final TxnFlagSet flags) {

    if (SHOULD_CHECK) {
      Objects.requireNonNull(flags);
    }
    this.proxy = proxy;
    this.keyVal = proxy.keyVal();
    this.readOnly = flags.isSet(MDB_RDONLY_TXN);
    if (env.isReadOnly() && !this.readOnly) {
      throw new EnvIsReadOnly();
    }
    this.env = env;
    this.parent = parent;
    if (parent != null && parent.isReadOnly() != this.readOnly) {
      throw new IncompatibleParent();
    }

    this.refCounterReleaser = env.acquire();
    try {
      final Pointer txnPtr = allocateDirect(RUNTIME, ADDRESS);
      final Pointer txnParentPtr = parent == null ? null : parent.ptr;
      checkRc(LIB.mdb_txn_begin(env.pointer(), txnParentPtr, flags.getMask(), txnPtr));
      ptr = txnPtr.getPointer(0);

      state = READY;
    } catch (final Exception e) {
      this.refCounterReleaser.release();
      throw e;
    }
  }

  /**
   * Aborts this transaction.
   *
   * <p>If this is a read-write transaction, and you have any open {@link Cursor}s against this
   * transaction, they <strong>MUST</strong> be closed first, else you will not be able to close the
   * cursor after this transaction has been committed.
   */
  public void abort() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    checkReady();
    state = DONE;
    LIB.mdb_txn_abort(ptr);

    // No call to refCounterReleaser.release() here because the keyVal is still open
    // and the txn can still be reset.
  }

  /**
   * Closes this transaction. Any uncommitted work will be aborted first.
   *
   * <p>If any {@link Cursor}s have been opened on this transaction, they <strong>MUST</strong> be
   * closed first, else you will not be able to close the cursor after its transaction has been
   * closed.
   *
   * <p>Closing the transaction will invoke {@link BufferProxy#deallocate(java.lang.Object)} for
   * each read-only buffer (ie the key and value).
   *
   * <p>If this is a read-write transaction, and you have any open {@link Cursor}s against this
   * transaction, they <strong>MUST</strong> be closed first, else you will not be able to close the
   * cursor after this transaction has been closed.
   */
  @Override
  public void close() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    if (state == RELEASED) {
      return;
    }
    if (state == READY) {
      LIB.mdb_txn_abort(ptr);
    }
    keyVal.close();
    state = RELEASED;

    refCounterReleaser.release();
  }

  /**
   * Commits this transaction.
   *
   * <p>If you have an open cursor using this transaction, you must close the cursor before
   * committing.
   *
   * <p>If this is a read-write transaction, and you have any open {@link Cursor}s against this
   * transaction, they <strong>MUST</strong> be closed first, else you will not be able to close the
   * cursor after this transaction has been committed.
   */
  public void commit() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    checkReady();
    state = DONE;
    checkRc(LIB.mdb_txn_commit(ptr));
  }

  /**
   * Return the transaction's ID.
   *
   * @return A transaction ID, valid if input is an active transaction
   */
  public long getId() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    return LIB.mdb_txn_id(ptr);
  }

  /**
   * Obtains this transaction's parent.
   *
   * @return the parent transaction (if present, i.e. may be null)
   */
  public Txn<T> getParent() {
    return parent;
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
   * Whether this transaction is writable (i.e. not read-only).
   *
   * @return if writable
   */
  public boolean isWritable() {
    return !readOnly;
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory. Any use of this
   * buffer must comply with the standard LMDB C "mdb_get" contract (ie do not modify, do not
   * attempt to release the memory, do not use once the transaction or cursor closes, do not use
   * after a write etc).
   *
   * @return the key buffer (never null)
   */
  public T key() {
    return keyVal.key();
  }

  /** Renews a read-only transaction previously released by {@link #reset()}. */
  public void renew() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    if (state != RESET) {
      throw new NotResetException();
    }
    state = DONE;
    checkRc(LIB.mdb_txn_renew(ptr));
    state = READY;
  }

  /**
   * Aborts this read-only transaction and resets the transaction handle, so it can be reused upon
   * calling {@link #renew()}.
   *
   * <p>Not applicable to write transactions.
   */
  public void reset() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    checkReadOnly();
    if (state != READY && state != DONE) {
      throw new ResetException();
    }
    state = RESET;
    LIB.mdb_txn_reset(ptr);
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory. Any use of this
   * buffer must comply with the standard LMDB C "mdb_get" contract (ie do not modify, do not
   * attempt to release the memory, do not use once the transaction or cursor closes, do not use
   * after a write etc).
   *
   * @return the value buffer (never null)
   */
  public T val() {
    return keyVal.val();
  }

  void checkReadOnly() {
    if (!readOnly) {
      throw new ReadOnlyRequiredException();
    }
  }

  void checkReady() {
    if (state != READY) {
      throw new NotReadyException();
    }
  }

  boolean isReady() {
    return state == READY;
  }

  void checkWritesAllowed() {
    if (readOnly) {
      throw new ReadWriteRequiredException();
    }
  }

  /**
   * Return the state of the transaction.
   *
   * @return the state
   */
  State getState() {
    return state;
  }

  KeyVal<T> kv() {
    return keyVal;
  }

  KeyVal<T> newKeyVal() {
    return proxy.keyVal();
  }

  Pointer pointer() {
    return ptr;
  }

  /** Transaction must abort, has a child, or is invalid. */
  public static final class BadException extends LmdbNativeException {

    static final int MDB_BAD_TXN = -30_782;
    private static final long serialVersionUID = 1L;

    BadException() {
      super(MDB_BAD_TXN, "Transaction must abort, has a child, or is invalid");
    }
  }

  /** Invalid reuse of reader locktable slot. */
  public static final class BadReaderLockException extends LmdbNativeException {

    static final int MDB_BAD_RSLOT = -30_783;
    private static final long serialVersionUID = 1L;

    BadReaderLockException() {
      super(MDB_BAD_RSLOT, "Invalid reuse of reader locktable slot");
    }
  }

  /** The proposed R-W transaction is incompatible with a R-O Env. */
  public static class EnvIsReadOnly extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public EnvIsReadOnly() {
      super("Read-write Txn incompatible with read-only Env");
    }
  }

  /** The proposed transaction is incompatible with its parent transaction. */
  public static class IncompatibleParent extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public IncompatibleParent() {
      super("Transaction incompatible with its parent transaction");
    }
  }

  /** Transaction is not in a READY state. */
  public static final class NotReadyException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public NotReadyException() {
      super(
          "Transaction is not in ready state, i.e. it has been closed/committed/aborted/reset. "
              + "You may see this if have you tried to close a cursor after committing the transaction, "
              + "or if you have tried to use a cursor after closing its transaction.");
    }
  }

  /** The current transaction has not been reset. */
  public static class NotResetException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public NotResetException() {
      super("Transaction has not been reset");
    }
  }

  /** The current transaction is not a read-only transaction. */
  public static class ReadOnlyRequiredException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public ReadOnlyRequiredException() {
      super("Not a read-only transaction");
    }
  }

  /** The current transaction is not a read-write transaction. */
  public static class ReadWriteRequiredException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public ReadWriteRequiredException() {
      super("Not a read-write transaction");
    }
  }

  /** The current transaction has already been reset. */
  public static class ResetException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public ResetException() {
      super("Transaction has already been reset");
    }
  }

  /** Transaction has too many dirty pages. */
  public static final class TxFullException extends LmdbNativeException {

    static final int MDB_TXN_FULL = -30_788;
    private static final long serialVersionUID = 1L;

    TxFullException() {
      super(MDB_TXN_FULL, "Transaction has too many dirty pages");
    }
  }

  /** Transaction states. */
  enum State {
    READY,
    DONE,
    RESET,
    RELEASED
  }
}
