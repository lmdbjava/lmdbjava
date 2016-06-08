package org.lmdbjava;

import java.io.File;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import org.lmdbjava.Library.MDB_stat;
import static org.lmdbjava.Library.lib;
import static org.lmdbjava.Library.runtime;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;

/**
 * LMDB environment.
 */
public final class Env implements AutoCloseable {

  private boolean closed = false;
  private boolean open = false;
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
   * Close the handle.
   * <p>
   * Will silently return if already closed or never opened.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (!open) {
      return;
    }
    lib.mdb_env_close(ptr);
  }

  /**
   * Sets the map size.
   *
   * @param mapSize new limit in bytes
   * @throws AlreadyOpenException   if the environment has already been opened
   * @throws AlreadyClosedException if already closed
   * @throws LmdbNativeException    if a native C error occurred
   */
  public void setMapSize(int mapSize) throws AlreadyOpenException,
                                             AlreadyClosedException,
                                             LmdbNativeException {
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    if (closed) {
      throw new AlreadyClosedException(Env.class.getSimpleName());
    }
    checkRc(lib.mdb_env_set_mapsize(ptr, mapSize));
  }

  /**
   * Sets the maximum number of databases permitted.
   *
   * @param dbs new limit
   * @throws AlreadyOpenException   if the environment has already been opened
   * @throws AlreadyClosedException if already closed
   * @throws LmdbNativeException    if a native C error occurred
   */
  public void setMaxDbs(int dbs) throws AlreadyOpenException,
                                        AlreadyClosedException,
                                        LmdbNativeException {
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    if (closed) {
      throw new AlreadyClosedException(Env.class.getSimpleName());
    }
    checkRc(lib.mdb_env_set_maxdbs(ptr, dbs));
  }

  /**
   * Sets the maximum number of databases permitted.
   *
   * @param readers new limit
   * @throws AlreadyOpenException   if the environment has already been opened
   * @throws AlreadyClosedException if already closed
   * @throws LmdbNativeException    if a native C error occurred
   */
  public void setMaxReaders(int readers) throws AlreadyOpenException,
                                                AlreadyClosedException,
                                                LmdbNativeException {
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    if (closed) {
      throw new AlreadyClosedException(Env.class.getSimpleName());
    }
    checkRc(lib.mdb_env_set_maxreaders(ptr, readers));
  }

  /**
   * Indicates whether this environment has been closed.
   *
   * @return true if closed
   */
  public boolean isClosed() {
    return closed;
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
   * @throws AlreadyOpenException   if already open
   * @throws AlreadyClosedException if already closed
   * @throws LmdbNativeException    if a native C error occurred
   */
  public void open(final File path, final int mode, final EnvFlags... flags)
      throws AlreadyOpenException, AlreadyClosedException, LmdbNativeException {
    requireNonNull(path);
    if (open) {
      throw new AlreadyOpenException(Env.class.getSimpleName());
    }
    if (closed) {
      throw new AlreadyClosedException(Env.class.getSimpleName());
    }
    final int flagsMask = mask(flags);
    checkRc(lib.mdb_env_open(ptr, path.getAbsolutePath(), flagsMask, mode));
    this.open = true;
  }

  /**
   * Return statistics about this environment.
   *
   * @return an immutable statistics object.
   * @throws NotOpenException    if the env has not been opened
   * @throws LmdbNativeException if a native C error occurred
   */
  public EnvStat stat() throws NotOpenException, LmdbNativeException {
    if (!open) {
      throw new NotOpenException(Env.class.getSimpleName());
    }
    final MDB_stat stat = new MDB_stat(runtime);
    checkRc(lib.mdb_env_stat(ptr, stat));
    return new EnvStat(
        stat.ms_psize.intValue(),
        stat.ms_depth.intValue(),
        stat.ms_branch_pages.longValue(),
        stat.ms_leaf_pages.longValue(),
        stat.ms_overflow_pages.longValue(),
        stat.ms_psize.longValue());
  }
}
