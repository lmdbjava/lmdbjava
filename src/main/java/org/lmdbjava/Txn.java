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
import jnr.ffi.provider.MemoryManager;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

/**
 * LMDB transaction.
 *
 * @param <T> buffer type
 */
public final class Txn<T> implements AutoCloseable {

  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();
  private boolean committed = false;
  private final Env<T> env;
  private final T key;
  private final Txn<T> parent;
  private BufferProxy<T> proxy;
  private final long ptrKeyAddr;
  private final long ptrValAddr;
  private final boolean readOnly;
  private boolean reset = false;
  private final T val;
  final Pointer ptr;
  final Pointer ptrKey;
  final Pointer ptrVal;

  Txn(final Env<T> env, final Txn<T> parent, final BufferProxy<T> proxy,
      final TxnFlags... flags)
      throws NotOpenException, IncompatibleParent, LmdbNativeException {
    requireNonNull(env);
    requireNonNull(proxy);
    if (!env.isOpen() || env.isClosed()) {
      throw new NotOpenException();
    }
    this.env = env;
    this.proxy = proxy;
    final int flagsMask = mask(flags);
    this.readOnly = isSet(flagsMask, MDB_RDONLY);
    this.parent = parent;
    if (parent != null) {
      if ((parent.readOnly && !this.readOnly)
              || (!parent.readOnly && this.readOnly)) {
        throw new IncompatibleParent();
      }
    }
    final Pointer txnPtr = allocateDirect(RUNTIME, ADDRESS);
    final Pointer txnParentPtr = parent == null ? null : parent.ptr;
    checkRc(LIB.mdb_txn_begin(env.ptr, txnParentPtr, flagsMask, txnPtr));
    ptr = txnPtr.getPointer(0);

    this.key = proxy.allocate();
    this.val = proxy.allocate();
    ptrKey = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrKeyAddr = ptrKey.address();
    ptrVal = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrValAddr = ptrVal.address();
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
    LIB.mdb_txn_abort(ptr);
    committed = true;
  }

  /**
   * Closes this transaction by aborting if not already committed.
   * <p>
   * Closing the transaction will invoke
   * {@link BufferProxy#deallocate(java.lang.Object)} for each read-only buffer
   * (ie the key and value) as well as any buffers allocated via
   * {@link #allocate(int)}. As such these buffers must not be used after the
   * transaction has closed.
   */
  @Override
  public void close() {
    if (committed) {
      return;
    }
    proxy.deallocate(key);
    proxy.deallocate(val);
    LIB.mdb_txn_abort(ptr);
    committed = true;
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
    checkRc(LIB.mdb_txn_commit(ptr));
    committed = true;
  }

  /**
   * Return the transaction's ID.
   *
   * @return A transaction ID, valid if input is an active transaction
   */
  public long getId() {
    return LIB.mdb_txn_id(ptr);
  }

  /**
   * Obtains this transaction's parent.
   *
   * @return the parent transaction (may be null)
   */
  public Txn<T> getParent() {
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
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory.
   * Any use of this buffer must comply with the standard LMDB C "mdb_get"
   * contract (ie do not modify, do not attempt to release the memory, do not
   * use once the transaction or cursor closes, do not use after a write etc).
   *
   * @return the key buffer (never null)
   */
  public T key() {
    return key;
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
    checkRc(LIB.mdb_txn_renew(ptr));
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
    LIB.mdb_txn_reset(ptr);
    reset = true;
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory.
   * Any use of this buffer must comply with the standard LMDB C "mdb_get"
   * contract (ie do not modify, do not attempt to release the memory, do not
   * use once the transaction or cursor closes, do not use after a write etc).
   *
   * @return the value buffer (never null)
   */
  public T val() {
    return val;
  }

  void checkNotCommitted() throws CommittedException {
    if (committed) {
      throw new CommittedException();
    }
  }

  void checkReadOnly() throws ReadOnlyRequiredException {
    if (!readOnly) {
      throw new ReadOnlyRequiredException();
    }
  }

  void checkWritesAllowed() throws ReadWriteRequiredException {
    if (readOnly) {
      throw new ReadWriteRequiredException();
    }
  }

  void keyIn(T key) {
    proxy.in(key, ptrKey, ptrKeyAddr);
  }

  void keyOut() {
    proxy.out(key, ptrKey, ptrKeyAddr);
  }

  void valIn(T val) {
    proxy.in(val, ptrVal, ptrValAddr);
  }

  void valOut() {
    proxy.out(val, ptrVal, ptrValAddr);
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
      super("Transaction has already been committed");
    }
  }

  /**
   * The proposed transaction is incompatible with its parent transaction.
   */
  public static class IncompatibleParent extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public IncompatibleParent() {
      super("Transaction incompatible with its parent transaction");
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
   * The current transaction is not a read-write transaction.
   */
  public static class ReadWriteRequiredException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public ReadWriteRequiredException() {
      super("Not a read-write transaction");
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
