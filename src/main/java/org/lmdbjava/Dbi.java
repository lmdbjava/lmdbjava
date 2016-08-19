/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import static java.util.Objects.requireNonNull;
import static jnr.ffi.Memory.allocateDirect;
import static jnr.ffi.NativeType.ADDRESS;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import org.lmdbjava.CursorIterator.IteratorType;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import org.lmdbjava.Library.MDB_stat;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.PutFlags.MDB_RESERVE;
import static org.lmdbjava.ResultCodeMapper.checkRc;

/**
 * LMDB Database.
 *
 * @param <T> buffer type
 */
public final class Dbi<T> {

  private final Env<T> env;
  private final String name;
  private final Pointer ptr;

  Dbi(final Env<T> env, final Txn<T> txn, final String name,
      final DbiFlags... flags) {
    this.env = env;
    this.name = name;
    final int flagsMask = mask(flags);
    final Pointer dbiPtr = allocateDirect(RUNTIME, ADDRESS);
    checkRc(LIB.mdb_dbi_open(txn.pointer(), name, flagsMask, dbiPtr));
    ptr = dbiPtr.getPointer(0);
  }

  /**
   * Close the database handle (normally unnecessary; use with caution).
   *
   * <p>
   * It is very rare that closing a database handle is useful. There are also
   * many warnings/restrictions if closing a database handle (refer to the LMDB
   * C documentation). As such this is non-routine usage and this class does not
   * track the open/closed state of the {@link Dbi}. Advanced users are expected
   * to have specific reasons for using this method and will manage their own
   * state accordingly.
   */
  public void close() {
    LIB.mdb_dbi_close(env.pointer(), ptr);
  }

  /**
   * Starts a new read-write transaction and deletes the key.
   *
   * @param key key to delete from the database (not null)
   * @see #delete(org.lmdbjava.Txn, java.lang.Object, java.lang.Object)
   */
  public void delete(final T key) {
    try (final Txn<T> txn = env.txnWrite()) {
      delete(txn, key);
      txn.commit();
    }
  }

  /**
   * Deletes the key using the passed transaction.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @see #delete(org.lmdbjava.Txn, java.lang.Object, java.lang.Object)
   */
  public void delete(final Txn<T> txn, final T key) {
    delete(txn, key, null);
  }

  /**
   * Removes key/data pairs from the database.
   *
   * <p>
   * If the database does not support sorted duplicate data items
   * ({@link DbiFlags#MDB_DUPSORT}) the value parameter is ignored. If the
   * database supports sorted duplicates and the value parameter is null, all of
   * the duplicate data items for the key will be deleted. Otherwise, if the
   * data parameter is non-null only the matching data item will be deleted.
   *
   * <p>
   * This function will throw {@link KeyNotFoundException} if the key/data pair
   * is not found.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @param val value to delete from the database (null permitted)
   */
  public void delete(final Txn<T> txn, final T key, final T val) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      txn.checkReady();
      txn.checkWritesAllowed();
    }

    txn.keyIn(key);

    if (val == null) {
      checkRc(LIB.mdb_del(txn.pointer(), ptr, txn.pointerKey(), null));
    } else {
      txn.valIn(val);
      checkRc(LIB
          .mdb_del(txn.pointer(), ptr, txn.pointerKey(), txn.pointerVal()));
    }
  }

  /**
   * Drops the data in this database, leaving the database open for further use.
   *
   * <p>
   * This method slightly differs from the LMDB C API in that it does not
   * provide support for also closing the DB handle. If closing the DB handle is
   * required, please see {@link #close()}.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   */
  public void drop(final Txn<T> txn) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    checkRc(LIB.mdb_drop(txn.pointer(), ptr, 0));
  }

  /**
   * Get items from a database, moving the {@link Txn#val()} to the value.
   *
   * <p>
   * This function retrieves key/data pairs from the database. The address and
   * length of the data associated with the specified \b key are returned in the
   * structure to which \b data refers. If the database supports duplicate keys
   * ({@link org.lmdbjava.DbiFlags#MDB_DUPSORT}) then the first data item for
   * the key will be returned. Retrieval of other items requires the use of
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
      txn.checkReady();
    }
    txn.keyIn(key);
    final int rc = LIB.mdb_get(txn.pointer(), ptr, txn.pointerKey(), txn.
                               pointerVal());
    if (rc == MDB_NOTFOUND) {
      return null;
    }
    checkRc(rc);
    return txn.valOut(); // marked as out in LMDB C docs
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
   * Iterate the database from the first item and forwards.
   *
   * @param txn transaction handle (not null; not committed)
   * @return iterator
   */
  public CursorIterator<T> iterate(final Txn<T> txn) {
    return iterate(txn, null, FORWARD);
  }

  /**
   * Iterate the database from the first/last item and forwards/backwards.
   *
   * @param txn  transaction handle (not null; not committed)
   * @param type direction of iterator
   * @return iterator
   */
  public CursorIterator<T> iterate(final Txn<T> txn, final IteratorType type) {
    return iterate(txn, null, type);
  }

  /**
   * Iterate the database from the first/last item and forwards/backwards by
   * first seeking to the provided key.
   *
   * @param txn  transaction handle (not null; not committed)
   * @param key  the key to search from.
   * @param type direction of iterator
   * @return iterator
   */
  public CursorIterator<T> iterate(final Txn<T> txn, final T key,
                                   final IteratorType type) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      txn.checkReady();
    }
    return new CursorIterator<>(openCursor(txn), key, type);
  }

  /**
   * Create a cursor handle.
   *
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
   * @param txn transaction handle (not null; not committed)
   * @return cursor handle
   */
  public Cursor<T> openCursor(final Txn<T> txn) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      txn.checkReady();
    }
    final PointerByReference cursorPtr = new PointerByReference();
    checkRc(LIB.mdb_cursor_open(txn.pointer(), ptr, cursorPtr));
    return new Cursor<>(cursorPtr.getValue(), txn);
  }

  /**
   * Starts a new read-write transaction and puts the key/data pair.
   *
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @see #put(org.lmdbjava.Txn, java.lang.Object, java.lang.Object,
   * org.lmdbjava.PutFlags...)
   */
  public void put(final T key, final T val) {
    try (final Txn<T> txn = env.txnWrite()) {
      put(txn, key, val);
      txn.commit();
    }
  }

  /**
   * Store a key/value pair in the database.
   *
   * <p>
   * This function stores key/data pairs in the database. The default behavior
   * is to enter the new key/data pair, replacing any previously existing key if
   * duplicates are disallowed, or adding a duplicate data item if duplicates
   * are allowed ({@link DbiFlags#MDB_DUPSORT}).
   *
   * @param txn   transaction handle (not null; not committed; must be R-W)
   * @param key   key to store in the database (not null)
   * @param val   value to store in the database (not null)
   * @param flags Special options for this operation
   */
  public void put(final Txn<T> txn, final T key, final T val,
                  final PutFlags... flags) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      requireNonNull(val);
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    txn.keyIn(key);
    txn.valIn(val);
    final int mask = mask(flags);
    checkRc(LIB.mdb_put(txn.pointer(), ptr, txn.pointerKey(), txn.pointerVal(),
                        mask));
    txn.valOut(); // marked as in,out in LMDB C docs
  }

  /**
   * Reserve space for data of the given size, but don't copy the given val.
   * Instead, return a pointer to the reserved space, which the caller can fill
   * in later - before the next update operation or the transaction ends. This
   * saves an extra memcpy if the data is being generated later. LMDB does
   * nothing else with this memory, the caller is expected to modify all of the
   * space requested.
   *
   * <p>
   * This flag must not be specified if the database was opened with MDB_DUPSORT
   *
   * @param txn  transaction handle (not null; not committed; must be R-W)
   * @param key  key to store in the database (not null)
   * @param size size of the value to be stored in the database
   * @return a buffer that can be used to modify the value
   */
  public T reserve(final Txn<T> txn, final T key, final int size) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    txn.keyIn(key);
    txn.valIn(size);
    final int mask = mask(MDB_RESERVE);
    checkRc(LIB.mdb_put(txn.pointer(), ptr, txn.pointerKey(), txn.pointerVal(),
                        mask));
    return txn.valOut(); // marked as in,out in LMDB C docs
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

  /**
   * The specified DBI was changed unexpectedly.
   */
  public static final class BadDbiException extends LmdbNativeException {

    static final int MDB_BAD_DBI = -30_780;
    private static final long serialVersionUID = 1L;

    BadDbiException() {
      super(MDB_BAD_DBI, "The specified DBI was changed unexpectedly");
    }
  }

  /**
   * Unsupported size of key/DB name/data, or wrong DUPFIXED size.
   */
  public static final class BadValueSizeException extends LmdbNativeException {

    static final int MDB_BAD_VALSIZE = -30_781;
    private static final long serialVersionUID = 1L;

    BadValueSizeException() {
      super(MDB_BAD_VALSIZE,
            "Unsupported size of key/DB name/data, or wrong DUPFIXED size");
    }
  }

  /**
   * Environment maxdbs reached.
   */
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

    static final int MDB_INCOMPATIBLE = -30_784;
    private static final long serialVersionUID = 1L;

    IncompatibleException() {
      super(MDB_INCOMPATIBLE,
            "Operation and DB incompatible, or DB type changed");
    }
  }

  /**
   * Key/data pair already exists.
   */
  public static final class KeyExistsException extends LmdbNativeException {

    static final int MDB_KEYEXIST = -30_799;
    private static final long serialVersionUID = 1L;

    KeyExistsException() {
      super(MDB_KEYEXIST, "key/data pair already exists");
    }
  }

  /**
   * Key/data pair not found (EOF).
   */
  public static final class KeyNotFoundException extends LmdbNativeException {

    static final int MDB_NOTFOUND = -30_798;
    private static final long serialVersionUID = 1L;

    KeyNotFoundException() {
      super(MDB_NOTFOUND, "key/data pair not found (EOF)");
    }
  }

  /**
   * Database contents grew beyond environment mapsize.
   */
  public static final class MapResizedException extends LmdbNativeException {

    static final int MDB_MAP_RESIZED = -30_785;
    private static final long serialVersionUID = 1L;

    MapResizedException() {
      super(MDB_MAP_RESIZED, "Database contents grew beyond environment mapsize");
    }
  }
}
