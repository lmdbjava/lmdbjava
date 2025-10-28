/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
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

import static java.util.Objects.requireNonNull;
import static jnr.ffi.Memory.allocateDirect;
import static jnr.ffi.NativeType.ADDRESS;
import static org.lmdbjava.Dbi.KeyExistsException.MDB_KEYEXIST;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.KeyRange.all;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.PutFlags.MDB_RESERVE;
import static org.lmdbjava.ResultCodeMapper.checkRc;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import jnr.ffi.Pointer;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.PointerByReference;
import org.lmdbjava.Library.ComparatorCallback;
import org.lmdbjava.Library.MDB_stat;

/**
 * LMDB Database.
 *
 * @param <T> buffer type
 */
public final class Dbi<T> {

  private final ComparatorCallback callbackComparator;
  private boolean cleaned;
  // Used for CursorIterable KeyRange testing and/or native callbacks
  private final Comparator<T> comparator;
  private final Env<T> env;
  private final byte[] name;
  private final Pointer ptr;
  private final BufferProxy<T> proxy;
  private final DbiFlagSet dbiFlagSet;

  Dbi(
      final Env<T> env,
      final Txn<T> txn,
      final byte[] name,
      final Comparator<T> comparator,
      final boolean nativeCb,
      final BufferProxy<T> proxy,
      final DbiFlagSet dbiFlagSet) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      txn.checkReady();
    }
    this.env = env;
    this.name = name == null ? null : Arrays.copyOf(name, name.length);
    this.proxy = proxy;
    this.comparator = comparator;
    this.dbiFlagSet = dbiFlagSet;
    final Pointer dbiPtr = allocateDirect(RUNTIME, ADDRESS);
    checkRc(LIB.mdb_dbi_open(txn.pointer(), name, dbiFlagSet.getMask(), dbiPtr));
    ptr = dbiPtr.getPointer(0);
    if (nativeCb) {
      requireNonNull(comparator, "comparator cannot be null if nativeCb is set");
      // LMDB will call back to this comparator for insertion/iteration order
      this.callbackComparator =
          (keyA, keyB) -> {
            final T compKeyA  = proxy.out(proxy.allocate(), keyA);
            final T compKeyB = proxy.out(proxy.allocate(), keyB);
            final int result = this.comparator.compare(compKeyA, compKeyB);
            proxy.deallocate(compKeyA);
            proxy.deallocate(compKeyB);
            return result;
          };
      LIB.mdb_set_compare(txn.pointer(), ptr, callbackComparator);
    } else {
      callbackComparator = null;
    }
  }

  Pointer pointer() {
    return ptr;
  }

  /**
   * Close the database handle (normally unnecessary; use with caution).
   *
   * <p>It is very rare that closing a database handle is useful. There are also many
   * warnings/restrictions if closing a database handle (refer to the LMDB C documentation). As such
   * this is non-routine usage and this class does not track the open/closed state of the {@link
   * Dbi}. Advanced users are expected to have specific reasons for using this method and will
   * manage their own state accordingly.
   */
  public void close() {
    clean();
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    LIB.mdb_dbi_close(env.pointer(), ptr);
  }

  /**
   * Starts a new read-write transaction and deletes the key.
   *
   * @param key key to delete from the database (not null)
   * @return true if the key/data pair was found, false otherwise
   * @see #delete(org.lmdbjava.Txn, java.lang.Object, java.lang.Object)
   */
  public boolean delete(final T key) {
    try (Txn<T> txn = env.txnWrite()) {
      final boolean ret = delete(txn, key);
      txn.commit();
      return ret;
    }
  }

  /**
   * Deletes the key using the passed transaction.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @return true if the key/data pair was found, false otherwise
   * @see #delete(org.lmdbjava.Txn, java.lang.Object, java.lang.Object)
   */
  public boolean delete(final Txn<T> txn, final T key) {
    return delete(txn, key, null);
  }

  /**
   * Removes key/data pairs from the database.
   *
   * <p>If the database does not support sorted duplicate data items ({@link DbiFlags#MDB_DUPSORT})
   * the value parameter is ignored. If the database supports sorted duplicates and the value
   * parameter is null, all of the duplicate data items for the key will be deleted. Otherwise, if
   * the data parameter is non-null only the matching data item will be deleted.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @param val value to delete from the database (null permitted)
   * @return true if the key/data pair was found, false otherwise
   */
  public boolean delete(final Txn<T> txn, final T key, final T val) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      txn.checkReady();
      txn.checkWritesAllowed();
    }

    final Pointer transientKey = txn.kv().keyIn(key);

    Pointer data = null;
    Pointer transientVal = null;
    if (val != null) {
      transientVal = txn.kv().valIn(val);
      data = txn.kv().pointerVal();
    }
    final int rc = LIB.mdb_del(txn.pointer(), ptr, txn.kv().pointerKey(), data);
    if (rc == MDB_NOTFOUND) {
      return false;
    }
    checkRc(rc);
    ReferenceUtil.reachabilityFence0(transientKey);
    ReferenceUtil.reachabilityFence0(transientVal);
    ReferenceUtil.reachabilityFence0(key);
    ReferenceUtil.reachabilityFence0(val);
    return true;
  }

  /**
   * Drops the data in this database, leaving the database open for further use.
   *
   * <p>This method slightly differs from the LMDB C API in that it does not provide support for
   * also closing the DB handle. If closing the DB handle is required, please see {@link #close()}.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   */
  public void drop(final Txn<T> txn) {
    drop(txn, false);
  }

  /**
   * Drops the database. If delete is set to true, the database will be deleted and handle will be
   * closed. See {@link #close()} for implication of handle close. Otherwise, only the data in this
   * database will be dropped.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param delete whether database should be deleted.
   */
  public void drop(final Txn<T> txn, final boolean delete) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      env.checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    if (delete) {
      clean();
    }
    final int del = delete ? 1 : 0;
    checkRc(LIB.mdb_drop(txn.pointer(), ptr, del));
  }

  /**
   * Get items from a database, moving the {@link Txn#val()} to the value.
   *
   * <p>This function retrieves key/data pairs from the database. The address and length of the data
   * associated with the specified \b key are returned in the structure to which \b data refers. If
   * the database supports duplicate keys ({@link org.lmdbjava.DbiFlags#MDB_DUPSORT}) then the first
   * data item for the key will be returned. Retrieval of other items requires the use of
   * #mdb_cursor_get().
   *
   * @param txn transaction handle (not null; not committed)
   * @param key key to search for in the database (not null)
   * @return the data or null if not found
   */
  public T get(final Txn<T> txn, final T key) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      env.checkNotClosed();
      txn.checkReady();
    }
    final Pointer transientKey = txn.kv().keyIn(key);
    final int rc = LIB.mdb_get(txn.pointer(), ptr, txn.kv().pointerKey(), txn.kv().pointerVal());
    if (rc == MDB_NOTFOUND) {
      return null;
    }
    checkRc(rc);
    final T result = txn.kv().valOut(); // marked as out in LMDB C docs
    ReferenceUtil.reachabilityFence0(transientKey);
    ReferenceUtil.reachabilityFence0(key);
    return result;
  }

  /**
   * Obtains the name of this database.
   *
   * @return the name (may be null)
   */
  public byte[] getName() {
    return name == null ? null : Arrays.copyOf(name, name.length);
  }

  public String getNameAsString() {
    return getNameAsString(Env.DEFAULT_NAME_CHARSET);
  }


  /**
   * Obtains the name of this database, using the supplied {@link Charset}.
   *
   * @return The name of the database. If this is the unnamed database an empty
   * string will be returned.
   * @throws RuntimeException if the name can't be decoded.
   */
  public String getNameAsString(final Charset charset) {
    if (name == null) {
      return "";
    } else {
      // Assume a UTF8 encoding as we don't know, thus swallow if it fails
      try {
        return new String(name, requireNonNull(charset));
      } catch (Exception e) {
        throw new RuntimeException("Unable to decode database name using charset " + charset);
      }
    }
  }

  /**
   * Iterate the database from the first item and forwards.
   *
   * @param txn transaction handle (not null; not committed)
   * @return iterator
   */
  public CursorIterable<T> iterate(final Txn<T> txn) {
    return iterate(txn, all());
  }

  /**
   * Iterate the database in accordance with the provided {@link KeyRange}.
   *
   * @param txn transaction handle (not null; not committed)
   * @param range range of acceptable keys (not null)
   * @return iterator (never null)
   */
  public CursorIterable<T> iterate(final Txn<T> txn, final KeyRange<T> range) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(range);
      env.checkNotClosed();
      txn.checkReady();
    }
    return new CursorIterable<>(txn, this, range, comparator, proxy);
  }

  /**
   * Return DbiFlags for this Dbi.
   *
   * @param txn transaction handle (not null; not committed)
   * @return the list of flags this Dbi was created with
   */
  public List<DbiFlags> listFlags(final Txn<T> txn) {
    // TODO we could just return what is in dbiFlagSet, rather than hitting LMDB.
    if (SHOULD_CHECK) {
      env.checkNotClosed();
    }
    final IntByReference resultPtr = new IntByReference();
    checkRc(LIB.mdb_dbi_flags(txn.pointer(), ptr, resultPtr));

    final int flags = resultPtr.intValue();

    final List<DbiFlags> result = new ArrayList<>();

    for (final DbiFlags flag : DbiFlags.values()) {
      if (isSet(flags, flag)) {
        result.add(flag);
      }
    }

    return result;
  }

  /**
   * Create a cursor handle.
   *
   * <p>A cursor is associated with a specific transaction and database. A cursor cannot be used
   * when its database handle is closed. Nor when its transaction has ended, except with {@link
   * Cursor#renew(org.lmdbjava.Txn)}. It can be discarded with {@link Cursor#close()}. A cursor in a
   * write-transaction can be closed before its transaction ends, and will otherwise be closed when
   * its transaction ends. A cursor in a read-only transaction must be closed explicitly, before or
   * after its transaction ends. It can be reused with {@link Cursor#renew(org.lmdbjava.Txn)} before
   * finally closing it.
   *
   * @param txn transaction handle (not null; not committed)
   * @return cursor handle
   */
  public Cursor<T> openCursor(final Txn<T> txn) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      env.checkNotClosed();
      txn.checkReady();
    }
    final PointerByReference cursorPtr = new PointerByReference();
    checkRc(LIB.mdb_cursor_open(txn.pointer(), ptr, cursorPtr));
    return new Cursor<>(cursorPtr.getValue(), txn, env);
  }

  /**
   * Starts a new read-write transaction and puts the key/data pair.
   *
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @see #put(Txn, Object, Object, PutFlagSet)
   */
  public void put(final T key, final T val) {
    try (Txn<T> txn = env.txnWrite()) {
      put(txn, key, val, PutFlagSet.EMPTY);
      txn.commit();
    }
  }

  /**
   * @deprecated Use {@link Dbi#put(Txn, Object, Object, PutFlagSet)} instead, with a statically
   * held {@link PutFlagSet}.
   * <hr>
   * <p>
   * Store a key/value pair in the database.
   * </p>
   * <p>This function stores key/data pairs in the database. The default behavior is to enter the
   * new key/data pair, replacing any previously existing key if duplicates are disallowed, or
   * adding a duplicate data item if duplicates are allowed ({@link DbiFlags#MDB_DUPSORT}).
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @param flags Special options for this operation
   * @return true if the value was put, false if MDB_NOOVERWRITE or MDB_NODUPDATA were set and the
   *     key/value existed already.
   */
  @Deprecated
  public boolean put(final Txn<T> txn, final T key, final T val, final PutFlags... flags) {
    return put(txn, key, val, PutFlagSet.of(flags));
  }

  /**
   * Store a key/value pair in the database.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @return true if the value was put, false if MDB_NOOVERWRITE or MDB_NODUPDATA were set and the
   *     key/value existed already.
   * @see #put(Txn, Object, Object, PutFlagSet)
   */
  public boolean put(final Txn<T> txn, final T key, final T val) {
    return put(txn, key, val, PutFlagSet.EMPTY);
  }

  /**
   * Store a key/value pair in the database.
   *
   * <p>This function stores key/data pairs in the database. The default behavior is to enter the
   * new key/data pair, replacing any previously existing key if duplicates are disallowed, or
   * adding a duplicate data item if duplicates are allowed ({@link DbiFlags#MDB_DUPSORT}).
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @param flags Special options for this operation.
   * @return true if the value was put, false if MDB_NOOVERWRITE or MDB_NODUPDATA were set and the
   *     key/value existed already.
   */
  public boolean put(final Txn<T> txn, final T key, final T val, final PutFlagSet flags) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      requireNonNull(val);
      env.checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    final PutFlagSet flagSet = flags != null ? flags : PutFlagSet.empty();
    final Pointer transientKey = txn.kv().keyIn(key);
    final Pointer transientVal = txn.kv().valIn(val);
    final int rc = LIB.mdb_put(txn.pointer(), ptr, txn.kv().pointerKey(), txn.kv().pointerVal(), flagSet.getMask());
    if (rc == MDB_KEYEXIST) {
      if (flagSet.isSet(MDB_NOOVERWRITE)) {
        txn.kv().valOut(); // marked as in,out in LMDB C docs
      } else if (!flagSet.isSet(MDB_NODUPDATA)) {
        checkRc(rc);
      }
      return false;
    }
    checkRc(rc);
    ReferenceUtil.reachabilityFence0(transientKey);
    ReferenceUtil.reachabilityFence0(transientVal);
    ReferenceUtil.reachabilityFence0(key);
    ReferenceUtil.reachabilityFence0(val);
    return true;
  }

  /**
   * Reserve space for data of the given size, but don't copy the given val. Instead, return a
   * pointer to the reserved space, which the caller can fill in later - before the next update
   * operation or the transaction ends. This saves an extra memcpy if the data is being generated
   * later. LMDB does nothing else with this memory, the caller is expected to modify all of the
   * space requested.
   *
   * <p>This flag must not be specified if the database was opened with MDB_DUPSORT
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to store in the database (not null)
   * @param size size of the value to be stored in the database
   * @param op options for this operation
   * @return a buffer that can be used to modify the value
   */
  public T reserve(final Txn<T> txn, final T key, final int size, final PutFlags... op) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      env.checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    final Pointer transientKey = txn.kv().keyIn(key);
    final Pointer transientVal = txn.kv().valIn(size);
    final int flags = mask(op) | MDB_RESERVE.getMask();
    checkRc(LIB.mdb_put(txn.pointer(), ptr, txn.kv().pointerKey(), txn.kv().pointerVal(), flags));
    txn.kv().valOut(); // marked as in,out in LMDB C docs
    ReferenceUtil.reachabilityFence0(transientKey);
    ReferenceUtil.reachabilityFence0(transientVal);
    ReferenceUtil.reachabilityFence0(key);
    return txn.val();
  }

  /**
   * Return statistics about this database.
   *
   * @param txn transaction handle (not null; not committed)
   * @return an immutable statistics object.
   */
  public Stat stat(final Txn<T> txn) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      env.checkNotClosed();
      txn.checkReady();
    }
    final MDB_stat stat = new MDB_stat(RUNTIME);
    checkRc(LIB.mdb_stat(txn.pointer(), ptr, stat));
    return new Stat(
        stat.f0_ms_psize.intValue(),
        stat.f1_ms_depth.intValue(),
        stat.f2_ms_branch_pages.longValue(),
        stat.f3_ms_leaf_pages.longValue(),
        stat.f4_ms_overflow_pages.longValue(),
        stat.f5_ms_entries.longValue());
  }

  private void clean() {
    if (cleaned) {
      return;
    }
    cleaned = true;
  }

  @Override
  public String toString() {
    String name;
    try {
      name = getNameAsString();
    } catch (Exception e) {
      name = "?";
    }
    return "Dbi{" +
            "name='" + name +
            "', dbiFlagSet=" + dbiFlagSet +
            '}';
  }

  /** The specified DBI was changed unexpectedly. */
  public static final class BadDbiException extends LmdbNativeException {

    static final int MDB_BAD_DBI = -30_780;
    private static final long serialVersionUID = 1L;

    BadDbiException() {
      super(MDB_BAD_DBI, "The specified DBI was changed unexpectedly");
    }
  }

  /** Unsupported size of key/DB name/data, or wrong DUPFIXED size. */
  public static final class BadValueSizeException extends LmdbNativeException {

    static final int MDB_BAD_VALSIZE = -30_781;
    private static final long serialVersionUID = 1L;

    BadValueSizeException() {
      super(MDB_BAD_VALSIZE, "Unsupported size of key/DB name/data, or wrong DUPFIXED size");
    }
  }

  /** Environment maxdbs reached. */
  public static final class DbFullException extends LmdbNativeException {

    static final int MDB_DBS_FULL = -30_791;
    private static final long serialVersionUID = 1L;

    DbFullException() {
      super(MDB_DBS_FULL, "Environment maxdbs reached");
    }
  }

  /**
   * Operation and DB incompatible, or DB type changed.
   *
   * <p>This can mean:
   *
   * <ul>
   *   <li>The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.
   *   <li>Opening a named DB when the unnamed DB has MDB_DUPSORT / MDB_INTEGERKEY.
   *   <li>Accessing a data record as a database, or vice versa.
   *   <li>The database was dropped and recreated with different flags.
   * </ul>
   */
  public static final class IncompatibleException extends LmdbNativeException {

    static final int MDB_INCOMPATIBLE = -30_784;
    private static final long serialVersionUID = 1L;

    IncompatibleException() {
      super(MDB_INCOMPATIBLE, "Operation and DB incompatible, or DB type changed");
    }
  }

  /** Key/data pair already exists. */
  public static final class KeyExistsException extends LmdbNativeException {

    static final int MDB_KEYEXIST = -30_799;
    private static final long serialVersionUID = 1L;

    KeyExistsException() {
      super(MDB_KEYEXIST, "key/data pair already exists");
    }
  }

  /** Key/data pair not found (EOF). */
  public static final class KeyNotFoundException extends LmdbNativeException {

    static final int MDB_NOTFOUND = -30_798;
    private static final long serialVersionUID = 1L;

    KeyNotFoundException() {
      super(MDB_NOTFOUND, "key/data pair not found (EOF)");
    }
  }

  /** Database contents grew beyond environment mapsize. */
  public static final class MapResizedException extends LmdbNativeException {

    static final int MDB_MAP_RESIZED = -30_785;
    private static final long serialVersionUID = 1L;

    MapResizedException() {
      super(MDB_MAP_RESIZED, "Database contents grew beyond environment mapsize");
    }
  }
}
