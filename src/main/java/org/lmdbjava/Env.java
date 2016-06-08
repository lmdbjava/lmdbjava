package org.lmdbjava;

import java.io.File;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.TransactionFlags.MDB_RDONLY;

/**
 * LMDB environment.
 */
public final class Env {

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
   * @param mode  Unix permissions to set on created files and semaphores
   * @param flags the flags for this new environment
   * @throws AlreadyOpenException if already open
   * @throws LmdbNativeException  if a native C error occurred
   */
  public void open(final File path, final int mode, final EnvFlags... flags)
      throws AlreadyOpenException, LmdbNativeException {
    requireNonNull(path);
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    final int flagsMask = mask(flags);
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
    return new Transaction(this, null, MDB_RDONLY);
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
    return new Transaction(this, null);
  }

}
