package org.lmdbjava;

import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.PointerByReference;
import org.lmdbjava.Env.NotOpenException;
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import org.lmdbjava.Txn.CommittedException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;
import static org.lmdbjava.ValueBuffers.createVal;
import static org.lmdbjava.ValueBuffers.wrap;

/**
 * LMDB Database.
 */
public final class Dbi {

  private final String name;
  final int dbi;
  final Env env;

  /**
   * Create and open an LMDB Database (dbi) handle.
   * <p>
   * The passed transaction will automatically commit and the database handle
   * will become available to other transactions.
   *
   * @param tx    transaction to open and commit this database within (required)
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @throws CommittedException  if already committed
   * @throws LmdbNativeException if a native C error occurred
   */
  public Dbi(final Txn tx, final String name, final DbiFlags... flags)
      throws CommittedException, LmdbNativeException {
    requireNonNull(tx);
    if (tx.isCommitted()) {
      throw new CommittedException();
    }
    this.env = tx.env;
    this.name = name;
    final int flagsMask = mask(flags);
    final IntByReference dbiPtr = new IntByReference();
    checkRc(lib.mdb_dbi_open(tx.ptr, name, flagsMask, dbiPtr));
    dbi = dbiPtr.intValue();
  }

  /**
   * @param key The key to delete from the database
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws NotOpenException         if the environment is not currently open
   * @throws LmdbNativeException      if a native C error occurred
   * @see #delete(Txn, ByteBuffer, ByteBuffer)
   */
  public void delete(final ByteBuffer key) throws
      CommittedException, BufferNotDirectException, LmdbNativeException,
      NotOpenException {
    try (Txn tx = new Txn(env)) {
      delete(tx, key);
      tx.commit();
    }
  }

  /**
   * @param tx  Transaction handle
   * @param key The key to delete from the database
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   * @see #delete(Txn, ByteBuffer, ByteBuffer)
   */
  public void delete(final Txn tx, final ByteBuffer key) throws
      CommittedException, BufferNotDirectException, LmdbNativeException {
    delete(tx, key, null);
  }

  /**
   * <p>
   * Removes key/data pairs from the database.
   * </p>
   * If the database does not support sorted duplicate data items
   * ({@link org.lmdbjava.DbiFlags#MDB_DUPSORT}) the value parameter is ignored.
   * If the database supports sorted duplicates and the data parameter is NULL,
   * all of the duplicate data items for the key will be deleted. Otherwise, if
   * the data parameter is non-NULL only the matching data item will be deleted.
   * This function will return false if the specified key/data pair is not in
   * the database.
   *
   * @param tx  Transaction handle
   * @param key The key to delete from the database
   * @param val The value to delete from the database
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   */
  public void delete(final Txn tx, final ByteBuffer key, final ByteBuffer val)
      throws
      CommittedException, BufferNotDirectException, LmdbNativeException {

    final MDB_val k = createVal(key);
    final MDB_val v = val == null ? null : createVal(key);

    checkRc(lib.mdb_del(tx.ptr, dbi, k, v));
  }

  /**
   * @param key The key to get from the database
   * @return A value placeholder for the memory address to be wrapped if found
   *         by key
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws NotOpenException         if the environment is not currently open
   * @throws LmdbNativeException      if a native C error occurred
   * @see #get(Txn, ByteBuffer)
   */
  public ByteBuffer get(final ByteBuffer key) throws
      CommittedException, BufferNotDirectException, LmdbNativeException,
      NotOpenException {
    try (Txn tx = new Txn(env, MDB_RDONLY)) {
      return get(tx, key);
    }
  }

  /**
   * <p>
   * Get items from a database.
   * </p>
   * <p>
   * This function retrieves key/data pairs from the database. The address and
   * length of the data associated with the specified \b key are returned in the
   * structure to which \b data refers. If the database supports duplicate keys
   * ({@link org.lmdbjava.DbiFlags#MDB_DUPSORT}) then the first data item for
   * the key will be returned. Retrieval of other items requires the use of
   * #mdb_cursor_get().
   *
   * @param tx  transaction handle
   * @param key The key to search for in the database
   * @return A value placeholder for the memory address to be wrapped if found
   *         by key
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   */
  public ByteBuffer get(final Txn tx, final ByteBuffer key) throws
      CommittedException, BufferNotDirectException, LmdbNativeException {
    assert key.isDirect();

    final MDB_val k = createVal(key);
    final MDB_val v = new MDB_val(runtime);

    checkRc(lib.mdb_get(tx.ptr, dbi, k, v));

    // inefficient as we create a BB
    final ByteBuffer bb = allocateDirect(1).order(LITTLE_ENDIAN);
    wrap(bb, v);
    return bb;
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
   * <p>
   * Create a cursor handle.
   * </p>
   * <p>
   * A cursor is associated with a specific transaction and database. A cursor
   * cannot be used when its database handle is closed. Nor when its transaction
   * has ended, except with #mdb_cursor_renew(). It can be discarded with
   * #mdb_cursor_close(). A cursor in a write-transaction can be closed before
   * its transaction ends, and will otherwise be closed when its transaction
   * ends. A cursor in a read-only transaction must be closed explicitly, before
   * or after its transaction ends. It can be reused with #mdb_cursor_renew()
   * before finally closing it.
   *
   * Earlier documentation said that cursors in every transaction were
   * closed when the transaction committed or aborted.
   *
   * @param tx transaction handle
   * @return cursor handle
   * @throws LmdbNativeException if a native C error occurred
   */
  public Cursor openCursor(final Txn tx) throws LmdbNativeException {
    PointerByReference ptr = new PointerByReference();
    checkRc(lib.mdb_cursor_open(tx.ptr, dbi, ptr));
    return new Cursor(ptr.getValue(), tx);
  }

  /**
   * @param key The key to store in the database
   * @param val The value to store in the database
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws NotOpenException         if the environment is not currently open
   * @throws LmdbNativeException      if a native C error occurred
   * @see #put(Txn, ByteBuffer, ByteBuffer, DbiFlags...)
   */
  public void put(final ByteBuffer key, final ByteBuffer val) throws
      CommittedException, BufferNotDirectException, LmdbNativeException,
      NotOpenException {
    try (Txn tx = new Txn(env)) {
      put(tx, key, val);
      tx.commit();
    }
  }

  /**
   * <p>
   * Store items into a database.
   * </p>
   * <p>
   * This function stores key/data pairs in the database. The default behavior
   * is to enter the new key/data pair, replacing any previously existing key if
   * duplicates are disallowed, or adding a duplicate data item if duplicates
   * are allowed ({@link org.lmdbjava.DbiFlags#MDB_DUPSORT}).
   *
   * @param tx    transaction handle
   * @param key   The key to store in the database
   * @param val   The value to store in the database
   * @param flags Special options for this operation
   * @throws CommittedException       if already committed
   * @throws BufferNotDirectException if a passed buffer is invalid
   * @throws LmdbNativeException      if a native C error occurred
   */
  public void put(final Txn tx, final ByteBuffer key, final ByteBuffer val,
                  final PutFlags... flags)
      throws CommittedException, BufferNotDirectException, LmdbNativeException {

    final MDB_val k = createVal(key);
    final MDB_val v = createVal(val);
    int mask = mask(flags);
    checkRc(lib.mdb_put(tx.ptr, dbi, k, v, mask));
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
