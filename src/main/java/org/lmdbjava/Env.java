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

import java.io.File;
import static java.lang.Boolean.getBoolean;
import java.nio.ByteBuffer;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.Library.LIB;
import org.lmdbjava.Library.MDB_envinfo;
import org.lmdbjava.Library.MDB_stat;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadWriteRequiredException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

/**
 * LMDB environment.
 *
 * @param <T> buffer type
 */
public final class Env<T> implements AutoCloseable {

  /**
   * Java system property name that can be set to disable optional checks.
   */
  public static final String DISABLE_CHECKS_PROP = "lmdbjava.disable.checks";

  /**
   * Indicates whether optional checks should be applied in LmdbJava. Optional
   * checks are only disabled in critical paths (see package-level JavaDocs).
   * Non-critical paths have optional checks performed at all times, regardless
   * of this property.
   */
  public static final boolean SHOULD_CHECK = !getBoolean(DISABLE_CHECKS_PROP);

  /**
   * Create an {@link Env} using the {@link ByteBufferProxy#PROXY_OPTIMAL}.
   *
   * @return the environment (never null)
   */
  public static Env<ByteBuffer> create() {
    return new Env<>(PROXY_OPTIMAL);
  }

  /**
   * Create an {@link Env} using the passed {@link BufferProxy}.
   *
   * @param <T>
   * @param proxy the proxy to use (required)
   * @return the environment (never null)
   */
  public static <T> Env<T> create(final BufferProxy<T> proxy) {
    return new Env<>(proxy);
  }

  private boolean closed = false;
  private boolean open = false;
  private final BufferProxy<T> proxy;
  final Pointer ptr;

  /**
   * Creates a new environment handle.
   */
  private Env(final BufferProxy<T> proxy) {
    requireNonNull(proxy);
    this.proxy = proxy;
    final PointerByReference envPtr = new PointerByReference();
    checkRc(LIB.mdb_env_create(envPtr));
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
    LIB.mdb_env_close(ptr);
  }

  /**
   * Copies an LMDB environment to the specified destination path.
   * <p>
   * This function may be used to make a backup of an existing environment. No
   * lockfile is created, since it gets recreated at need.
   * <p>
   * Note: This call can trigger significant file size growth if run in parallel
   * with write transactions, because it employs a read-only transaction. See
   * long-lived transactions under "Caveats" in the LMDB native documentation.
   *
   * @param path  destination directory, which must exist, be writable and empty
   * @param flags special options for this copy
   */
  public void copy(final File path, final CopyFlags... flags) {
    requireNonNull(path);
    if (!path.exists()) {
      throw new InvalidCopyDestination("Path must exist");
    }
    if (!path.isDirectory()) {
      throw new InvalidCopyDestination("Path must be a directory");
    }
    if (path.list().length > 0) {
      throw new InvalidCopyDestination("Path must contain no files");
    }
    final int flagsMask = mask(flags);
    checkRc(LIB.mdb_env_copy2(ptr, path.getAbsolutePath(), flagsMask));
  }

  /**
   * Sets the map size.
   *
   * @param mapSize new limit in bytes
   */
  public void setMapSize(final long mapSize) {
    if (open) {
      throw new AlreadyOpenException();
    }
    if (closed) {
      throw new AlreadyClosedException();
    }
    checkRc(LIB.mdb_env_set_mapsize(ptr, mapSize));
  }

  /**
   * Sets the map size.
   *
   * @param size the size in given unit.
   * @param unit the unit to use for the size.
   */
  public void setMapSize(final int size, ByteUnit unit) {
    setMapSize(unit.toBytes(size));
  }

  /**
   * Sets the maximum number of databases (ie {@link Dbi}s permitted.
   *
   * @param dbs new limit
   */
  public void setMaxDbs(final int dbs) {
    if (open) {
      throw new AlreadyOpenException();
    }
    if (closed) {
      throw new AlreadyClosedException();
    }
    checkRc(LIB.mdb_env_set_maxdbs(ptr, dbs));
  }

  /**
   * Sets the maximum number of databases permitted.
   *
   * @param readers new limit
   */
  public void setMaxReaders(final int readers) {
    if (open) {
      throw new AlreadyOpenException();
    }
    if (closed) {
      throw new AlreadyClosedException();
    }
    checkRc(LIB.mdb_env_set_maxreaders(ptr, readers));
  }

  /**
   * Return information about this environment.
   *
   * @return an immutable information object.
   */
  public EnvInfo info() {
    if (closed) {
      throw new AlreadyClosedException();
    }
    if (!open) {
      throw new NotOpenException();
    }
    final MDB_envinfo info = new MDB_envinfo(RUNTIME);
    checkRc(LIB.mdb_env_info(ptr, info));

    final long mapAddress;
    if (info.f0_me_mapaddr.get() == null) {
      mapAddress = 0;
    } else {
      mapAddress = info.f0_me_mapaddr.get().address();
    }

    return new EnvInfo(
        mapAddress,
        info.f1_me_mapsize.longValue(),
        info.f2_me_last_pgno.longValue(),
        info.f3_me_last_txnid.longValue(),
        info.f4_me_maxreaders.intValue(),
        info.f5_me_numreaders.intValue());
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
   */
  public void open(final File path, final int mode, final EnvFlags... flags) {
    requireNonNull(path);
    if (closed) {
      throw new AlreadyClosedException();
    }
    if (open) {
      throw new AlreadyOpenException();
    }
    final int flagsMask = mask(flags);
    checkRc(LIB.mdb_env_open(ptr, path.getAbsolutePath(), flagsMask, mode));
    this.open = true;
  }

  /**
   * Open the {@link Dbi}.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  public Dbi<T> openDbi(final String name, final DbiFlags... flags) {
    try (Txn<T> txn = txnWrite()) {
      final Dbi<T> dbi = new Dbi<>(this, txn, name, flags);
      txn.commit();
      return dbi;
    } catch (ReadWriteRequiredException | CommittedException e) {
      throw new IllegalStateException(e); // cannot happen (Txn is try scoped)
    }
  }

  /**
   * Return statistics about this environment.
   *
   * @return an immutable statistics object.
   */
  public EnvStat stat() {
    if (closed) {
      throw new AlreadyClosedException();
    }
    if (!open) {
      throw new NotOpenException();
    }
    final MDB_stat stat = new MDB_stat(RUNTIME);
    checkRc(LIB.mdb_env_stat(ptr, stat));
    return new EnvStat(
        stat.f0_ms_psize.intValue(),
        stat.f1_ms_depth.intValue(),
        stat.f2_ms_branch_pages.longValue(),
        stat.f3_ms_leaf_pages.longValue(),
        stat.f4_ms_overflow_pages.longValue(),
        stat.f5_ms_entries.longValue());
  }

  /**
   * Flushes the data buffers to disk.
   *
   * @param force force a synchronous flush (otherwise if the environment has
   *              the MDB_NOSYNC flag set the flushes will be omitted, and with
   *              MDB_MAPASYNC they will be asynchronous)
   */
  public void sync(final boolean force) {
    if (closed) {
      throw new AlreadyClosedException();
    }
    if (!open) {
      throw new NotOpenException();
    }
    final int f = force ? 1 : 0;
    checkRc(LIB.mdb_env_sync(ptr, f));
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @param flags  applicable flags (eg for a reusable, read-only transaction)
   * @return a transaction (never null)
   */
  public Txn<T> txn(final Txn<T> parent, final TxnFlags... flags) {
    return new Txn<>(this, parent, proxy, flags);
  }

  /**
   * Obtain a read-only transaction.
   *
   * @return
   */
  public Txn<T> txnRead() {
    return new Txn<>(this, null, proxy, MDB_RDONLY);
  }

  /**
   * Obtain a read-write transaction.
   *
   * @return
   */
  public Txn<T> txnWrite() {
    return new Txn<>(this, null, proxy);
  }

  /**
   * Object has already been closed and the operation is therefore prohibited.
   */
  public static final class AlreadyClosedException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public AlreadyClosedException() {
      super("Environment has already been closed");
    }
  }

  /**
   * Object has already been opened and the operation is therefore prohibited.
   */
  public static final class AlreadyOpenException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public AlreadyOpenException() {
      super("Environment has already been opened");
    }
  }

  /**
   * File is not a valid LMDB file.
   */
  public static final class FileInvalidException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_INVALID = -30_793;

    FileInvalidException() {
      super(MDB_INVALID, "File is not a valid LMDB file");
    }
  }

  /**
   * The specified copy destination is invalid.
   */
  public static class InvalidCopyDestination extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * <p>
     * @param message the reason
     */
    public InvalidCopyDestination(final String message) {
      super(message);
    }
  }

  /**
   * Environment mapsize reached.
   */
  public static final class MapFullException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_MAP_FULL = -30_792;

    MapFullException() {
      super(MDB_MAP_FULL, "Environment mapsize reached");
    }
  }

  /**
   * Object has is not open (eg never opened, or since closed).
   */
  public static class NotOpenException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public NotOpenException() {
      super("Environment is not open");
    }
  }

  /**
   * Environment maxreaders reached.
   */
  public static final class ReadersFullException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_READERS_FULL = -30_790;

    ReadersFullException() {
      super(MDB_READERS_FULL, "Environment maxreaders reached");
    }
  }

  /**
   * Environment version mismatch.
   */
  public static final class VersionMismatchException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;
    static final int MDB_VERSION_MISMATCH = -30_794;

    VersionMismatchException() {
      super(MDB_VERSION_MISMATCH, "Environment version mismatch");
    }
  }
}
