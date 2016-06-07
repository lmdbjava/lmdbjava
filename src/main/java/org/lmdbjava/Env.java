package org.lmdbjava;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;

import static java.util.Objects.requireNonNull;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.TransactionFlags.MDB_RDONLY;
import static org.lmdbjava.ResultCodeMapper.checkRc;

/**
 * LMDB environment.
 */
public final class Env {

  private static final Set<TransactionFlags> TX_FLAGS_RO = new HashSet<>();
  private static final Set<TransactionFlags> TX_FLAGS_RW = new HashSet<>();

  static {
    TX_FLAGS_RO.add(MDB_RDONLY);
  }

  private boolean open;
  final Pointer ptr;

  /**
   * Creates a new {@link MDB_env}.
   *
   * @throws LmdbNativeException if a native C error occurred
   */
  public Env() throws LmdbNativeException {
    final PointerByReference envPtr = new PointerByReference();
    checkRc(lib.mdb_env_create(envPtr));
    ptr = envPtr.getValue();
  }

  /**
   * Sets the map size.
   *
   * @param mapSize new limit in bytes
   * @throws AlreadyOpenException if the environment has already been opened
   * @throws LmdbNativeException  if a native C error occurred
   */
  public void setMapSize(int mapSize) throws AlreadyOpenException,
                                             LmdbNativeException {
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    checkRc(lib.mdb_env_set_mapsize(ptr, mapSize));
  }

  /**
   * Sets the maximum number of databases permitted.
   *
   * @param dbs new limit
   * @throws AlreadyOpenException if the environment has already been opened
   * @throws LmdbNativeException  if a native C error occurred
   */
  public void setMaxDbs(int dbs) throws AlreadyOpenException,
                                        LmdbNativeException {
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    checkRc(lib.mdb_env_set_maxdbs(ptr, dbs));
  }

  /**
   * Sets the maximum number of databases permitted.
   *
   * @param readers new limit
   * @throws AlreadyOpenException if the environment has already been opened
   * @throws LmdbNativeException  if a native C error occurred
   */
  public void setMaxReaders(int readers) throws AlreadyOpenException,
                                                LmdbNativeException {
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    checkRc(lib.mdb_env_set_maxreaders(ptr, readers));
  }

  /**
   * Indicates whether this environment has been opened.
   *
   * @return true if opened
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Opens the environment.
   *
   * @param path  file system destination
   * @param flags the flags for this new environment
   * @param mode  Unix permissions to set on created files and semaphores
   * @throws AlreadyOpenException if already open
   * @throws LmdbNativeException  if a native C error occurred
   */
  public void open(final File path,
                   final Set<EnvFlags> flags, final int mode) throws
      AlreadyOpenException,
      LmdbNativeException {
    requireNonNull(path);
    requireNonNull(flags);
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    final int flagsMask = MaskedFlag.mask(flags);
    checkRc(lib.mdb_env_open(ptr, path.getAbsolutePath(), flagsMask, mode));
    this.open = true;
  }

  /**
   * Begins a read-only transaction.
   *
   * @return the transaction (never null)
   * @throws NotOpenException    if the environment is not yet opened
   * @throws LmdbNativeException if a native C error occurred
   */
  public Transaction txnBeginReadOnly() throws NotOpenException,
                                               LmdbNativeException {
    return new Transaction(this, null, TX_FLAGS_RO);
  }

  /**
   * Begins a read-write transaction.
   *
   * @return the transaction (never null)
   * @throws NotOpenException    if the environment is not yet opened
   * @throws LmdbNativeException if a native C error occurred
   */
  public Transaction txnBeginReadWrite() throws NotOpenException,
                                                LmdbNativeException {
    return new Transaction(this, null, TX_FLAGS_RW);
  }

}
