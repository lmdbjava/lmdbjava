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

import java.nio.ByteBuffer;
import static java.util.Objects.requireNonNull;
import static jnr.ffi.Memory.allocateDirect;
import static jnr.ffi.NativeType.ADDRESS;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.provider.MemoryManager;
import org.lmdbjava.BufferProxy.BufferProxyFactory;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadWriteRequiredException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

/**
 * LMDB Database.
 * <p>
 * All accessor and mutator methods on this class use <code>ByteBuffer</code>.
 * This is solely for end user convenience and use of these methods is strongly
 * discouraged. Instead use the more flexible buffer API and lower latency
 * operating modes available via {@link #openCursor(org.lmdbjava.Txn)}.
 *
 * @param <T> buffer type
 */
public final class Dbi<T> {

  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();

  private final String name;
  private final BufferProxy<T> proxy;
  private final BufferProxyFactory<T> proxyFactory;
  private final Pointer ptrKey;
  private final long ptrKeyAddr;
  private final Pointer ptrVal;
  private final long ptrValAddr;
  private final T roKey;
  private final T roVal;
  final Pointer dbi;
  final Env env;

  /**
   * Create and open an LMDB Database (dbi) handle.
   * <p>
   * The passed transaction will automatically commit and the database handle
   * will become available to other transactions.
   *
   * @param tx           transaction to open and commit this database within
   *                     (not null; not committed; must be R-W)
   * @param name         name of the database (or null if no name is required)
   * @param proxyFactory buffer proxy factory (not null)
   * @param flags        to open the database with
   * @throws CommittedException         if already committed
   * @throws LmdbNativeException        if a native C error occurred
   * @throws ReadWriteRequiredException if a read-only transaction presented
   */
  public Dbi(final Txn tx, final String name,
             final BufferProxyFactory<T> proxyFactory, final DbiFlags... flags)
      throws CommittedException, LmdbNativeException, ReadWriteRequiredException {
    requireNonNull(tx);
    requireNonNull(proxyFactory);
    tx.checkNotCommitted();
    tx.checkWritesAllowed();
    this.env = tx.env;
    this.name = name;
    this.proxyFactory = proxyFactory;
    this.proxy = proxyFactory.create();
    final int flagsMask = mask(flags);
    final Pointer dbiPtr = allocateDirect(RUNTIME, ADDRESS);
    checkRc(LIB.mdb_dbi_open(tx.ptr, name, flagsMask, dbiPtr));
    dbi = dbiPtr.getPointer(0);
    this.roKey = proxy.allocate(0);
    this.roVal = proxy.allocate(0);
    ptrKey = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrKeyAddr = ptrKey.address();
    ptrVal = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrValAddr = ptrVal.address();
  }

  /**
   * Allocate a new buffer suitable for read-write use.
   *
   * @param bytes the size of the buffer
   * @return a writable buffer suitable for use with buffer-requiring methods
   */
  public T allocate(final int bytes) {
    return proxy.allocate(bytes);
  }

  /**
   * Starts a new read-write transaction and deletes the key.
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param key key to delete from the database (not null)
   * @throws CommittedException         if already committed
   * @throws NotOpenException           if the environment is not currently open
   * @throws LmdbNativeException        if a native C error occurred
   * @throws ReadWriteRequiredException if a read-only transaction presented
   * @see #delete(Txn, ByteBuffer, ByteBuffer)
   */
  public void delete(final T key) throws
      CommittedException, LmdbNativeException,
      NotOpenException, ReadWriteRequiredException {
    try (final Txn tx = new Txn(env)) {
      delete(tx, key);
      tx.commit();
    }
  }

  /**
   * Deletes the key using the passed transaction.
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param tx  transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @throws CommittedException         if already committed
   * @throws LmdbNativeException        if a native C error occurred
   * @throws ReadWriteRequiredException if a read-only transaction presented
   * @see #delete(Txn, ByteBuffer, ByteBuffer)
   */
  public void delete(final Txn tx, final T key) throws
      CommittedException, LmdbNativeException,
      ReadWriteRequiredException {
    delete(tx, key, null);
  }

  /**
   * Removes key/data pairs from the database.
   * <p>
   * If the database does not support sorted duplicate data items
   * ({@link DbiFlags#MDB_DUPSORT}) the value parameter is ignored. If the
   * database supports sorted duplicates and the value parameter is null, all of
   * the duplicate data items for the key will be deleted. Otherwise, if the
   * data parameter is non-null only the matching data item will be deleted.
   * <p>
   * This function will throw {@link KeyNotFoundException} if the key/data pair
   * is not found.
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param tx  transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @param val value to delete from the database (null permitted)
   * @throws CommittedException         if already committed
   * @throws LmdbNativeException        if a native C error occurred
   * @throws ReadWriteRequiredException if a read-only transaction presented
   */
  public void delete(final Txn tx, final T key, final T val)
      throws
      CommittedException, LmdbNativeException,
      ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      requireNonNull(tx);
      requireNonNull(key);
      tx.checkNotCommitted();
      tx.checkWritesAllowed();
    }

    proxy.set(key, ptrKey, ptrKeyAddr);

    if (val == null) {
      checkRc(LIB.mdb_del(tx.ptr, dbi, ptrKey, null));
    } else {
      proxy.set(val, ptrVal, ptrValAddr);
      checkRc(LIB.mdb_del(tx.ptr, dbi, ptrKey, ptrVal));
    }
  }

  /**
   * Gets an item from the database.
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param key key to get from the database (not null)
   * @return the value found
   * @throws CommittedException  if already committed
   * @throws NotOpenException    if the environment is not currently open
   * @throws LmdbNativeException if a native C error occurred
   * @see #get(Txn, ByteBuffer)
   */
  public T get(final T key) throws
      CommittedException, LmdbNativeException,
      NotOpenException {
    try (final Txn tx = new Txn(env, MDB_RDONLY)) {
      return get(tx, key);
    }
  }

  /**
   * Get items from a database, pointing the passed value buffer at the result.
   * <p>
   * This function retrieves key/data pairs from the database. The address and
   * length of the data associated with the specified \b key are returned in the
   * structure to which \b data refers. If the database supports duplicate keys
   * ({@link org.lmdbjava.DbiFlags#MDB_DUPSORT}) then the first data item for
   * the key will be returned. Retrieval of other items requires the use of
   * #mdb_cursor_get().
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param tx  transaction handle (not null; not committed)
   * @param key key to search for in the database (not null)
   * @return
   * @throws CommittedException  if already committed
   * @throws LmdbNativeException if a native C error occurred
   */
  public T get(final Txn tx, final T key)
      throws
      CommittedException, LmdbNativeException {
    if (SHOULD_CHECK) {
      requireNonNull(tx);
      requireNonNull(key);
      tx.checkNotCommitted();
    }
    proxy.set(key, ptrKey, ptrKeyAddr);
    checkRc(LIB.mdb_get(tx.ptr, dbi, ptrKey, ptrVal));
    proxy.dirty(roVal, ptrVal, ptrValAddr); // marked as out in LMDB C docs
    return roVal;
  }

  /**
   * Obtains the name of this database.
   *
   * @return the name (may be null or empty)
   */
  public String getName() {
    return name;
  }

  /**
   * Create a cursor handle.
   * <p>
   * Cursors are <em>strongly recommended</em> for all use cases, particularly
   * if flexible buffer implementations and/or latency is of concern.
   * <p>
   * A cursor is associated with a specific transaction and database. A cursor
   * cannot be used when its database handle is closed. Nor when its transaction
   * has ended, except with {@link Cursor#renew(org.lmdbjava.Txn)}. It can be
   * discarded with {@link Cursor#close()}. A cursor in a write-transaction can
   * be closed before its transaction ends, and will otherwise be closed when
   * its transaction ends. A cursor in a read-only transaction must be closed
   * explicitly, before or after its transaction ends. It can be reused with
   * {@link Cursor#renew(org.lmdbjava.Txn)} before finally closing it.
   *
   * @param tx transaction handle (not null; not committed)
   * @return cursor handle
   * @throws LmdbNativeException if a native C error occurred
   * @throws CommittedException  if already committed
   */
  public Cursor<T> openCursor(final Txn tx)
      throws
      LmdbNativeException, CommittedException {
    if (SHOULD_CHECK) {
      requireNonNull(tx);
      tx.checkNotCommitted();
    }
    final PointerByReference ptr = new PointerByReference();
    checkRc(LIB.mdb_cursor_open(tx.ptr, dbi, ptr));
    return new Cursor<>(ptr.getValue(), tx, proxyFactory.create());
  }

  /**
   * Starts a new read-write transaction and puts the key/data pair.
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @throws CommittedException         if already committed
   * @throws NotOpenException           if the environment is not currently open
   * @throws LmdbNativeException        if a native C error occurred
   * @throws ReadWriteRequiredException if a read-only transaction presented
   * @see Dbi#put(Txn, ByteBuffer, ByteBuffer, PutFlags...)
   */
  public void put(final T key, final T val) throws
      CommittedException, LmdbNativeException,
      NotOpenException, ReadWriteRequiredException {
    try (final Txn tx = new Txn(env)) {
      put(tx, key, val);
      tx.commit();
    }
  }

  /**
   * Store a key/value pair in the database.
   * <p>
   * This function stores key/data pairs in the database. The default behavior
   * is to enter the new key/data pair, replacing any previously existing key if
   * duplicates are disallowed, or adding a duplicate data item if duplicates
   * are allowed ({@link DbiFlags#MDB_DUPSORT}).
   * <p>
   * WARNING: Convenience method. Do not use if latency sensitive.
   *
   * @param tx    transaction handle (not null; not committed; must be R-W)
   * @param key   key to store in the database (not null)
   * @param val   value to store in the database (not null)
   * @param flags Special options for this operation
   * @throws CommittedException         if already committed
   * @throws LmdbNativeException        if a native C error occurred
   * @throws ReadWriteRequiredException if a read-only transaction presented
   */
  public void put(final Txn tx, final T key, final T val,
                  final PutFlags... flags)
      throws CommittedException, LmdbNativeException,
             ReadWriteRequiredException {
    if (SHOULD_CHECK) {
      requireNonNull(tx);
      requireNonNull(key);
      requireNonNull(val);
      tx.checkNotCommitted();
      tx.checkWritesAllowed();
    }
    proxy.set(key, ptrKey, ptrKeyAddr);
    proxy.set(val, ptrVal, ptrValAddr);
    int mask = mask(flags);
    checkRc(LIB.mdb_put(tx.ptr, dbi, ptrKey, ptrVal, mask));
    proxy.dirty(roVal, ptrVal, ptrValAddr); // marked as in,out in LMDB C docs   
  }

  /**
   * The specified DBI was changed unexpectedly.
   */
  public static final class BadDbiException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_BAD_DBI = -30_780;

    BadDbiException() {
      super(MDB_BAD_DBI, "The specified DBI was changed unexpectedly");
    }
  }

  /**
   * Unsupported size of key/DB name/data, or wrong DUPFIXED size.
   */
  public static final class BadValueSizeException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_BAD_VALSIZE = -30_781;

    BadValueSizeException() {
      super(MDB_BAD_VALSIZE,
            "Unsupported size of key/DB name/data, or wrong DUPFIXED size");
    }
  }

  /**
   * Environment maxdbs reached.
   */
  public static final class DbFullException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_DBS_FULL = -30_791;

    DbFullException() {
      super(MDB_DBS_FULL, "Environment maxdbs reached");
    }
  }

  /**
   * Operation and DB incompatible, or DB type changed.
   * <p>
   * This can mean:
   * <ul>
   * <li>The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.</li>
   * <li>Opening a named DB when the unnamed DB has MDB_DUPSORT /
   * MDB_INTEGERKEY.</li>
   * <li>Accessing a data record as a database, or vice versa.</li>
   * <li>The database was dropped and recreated with different flags.</li>
   * </ul>
   */
  public static final class IncompatibleException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_INCOMPATIBLE = -30_784;

    IncompatibleException() {
      super(MDB_INCOMPATIBLE,
            "Operation and DB incompatible, or DB type changed");
    }
  }

  /**
   * Key/data pair already exists.
   */
  public static final class KeyExistsException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_KEYEXIST = -30_799;

    KeyExistsException() {
      super(MDB_KEYEXIST, "key/data pair already exists");
    }
  }

  /**
   * Key/data pair not found (EOF).
   */
  public static final class KeyNotFoundException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_NOTFOUND = -30_798;

    KeyNotFoundException() {
      super(MDB_NOTFOUND, "key/data pair not found (EOF)");
    }
  }

  /**
   * Database contents grew beyond environment mapsize.
   */
  public static final class MapResizedException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_MAP_RESIZED = -30_785;

    MapResizedException() {
      super(MDB_MAP_RESIZED, "Database contents grew beyond environment mapsize");
    }
  }
}
