/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
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

import java.io.File;
import static java.lang.Boolean.getBoolean;
import java.nio.ByteBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.Library.LIB;
import org.lmdbjava.Library.MDB_envinfo;
import org.lmdbjava.Library.MDB_stat;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.TxnFlags.MDB_RDONLY_TXN;

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

  private boolean closed;
  private final int maxKeySize;
  private final BufferProxy<T> proxy;
  private final Pointer ptr;
  private final boolean readOnly;

  private Env(final BufferProxy<T> proxy, final Pointer ptr,
              final boolean readOnly) {
    this.proxy = proxy;
    this.readOnly = readOnly;
    this.ptr = ptr;
    // cache max key size to avoid further JNI calls
    this.maxKeySize = LIB.mdb_env_get_maxkeysize(ptr);
  }

  /**
   * Create an {@link Env} using the {@link ByteBufferProxy#PROXY_OPTIMAL}.
   *
   * @return the environment (never null)
   */
  public static Builder<ByteBuffer> create() {
    return new Builder<>(PROXY_OPTIMAL);
  }

  /**
   * Create an {@link Env} using the passed {@link BufferProxy}.
   *
   * @param <T>   buffer type
   * @param proxy the proxy to use (required)
   * @return the environment (never null)
   */
  public static <T> Builder<T> create(final BufferProxy<T> proxy) {
    return new Builder<>(proxy);
  }

  /**
   * Opens an environment with a single default database in 0664 mode using the
   * {@link ByteBufferProxy#PROXY_OPTIMAL}.
   *
   * @param path  file system destination
   * @param size  size in megabytes
   * @param flags the flags for this new environment
   * @return env the environment (never null)
   */
  public static Env<ByteBuffer> open(final File path, final int size,
                                     final EnvFlags... flags) {
    return new Builder<>(PROXY_OPTIMAL)
        .setMapSize(size * 1_024L * 1_024L)
        .open(path, flags);
  }

  /**
   * Close the handle.
   *
   * <p>
   * Will silently return if already closed or never opened.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    LIB.mdb_env_close(ptr);
  }

  /**
   * Copies an LMDB environment to the specified destination path.
   *
   * <p>
   * This function may be used to make a backup of an existing environment. No
   * lockfile is created, since it gets recreated at need.
   *
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
    final String[] files = path.list();
    if (files != null && files.length > 0) {
      throw new InvalidCopyDestination("Path must contain no files");
    }
    final int flagsMask = mask(flags);
    checkRc(LIB.mdb_env_copy2(ptr, path.getAbsolutePath(), flagsMask));
  }

  /**
   * Get the maximum size of keys and MDB_DUPSORT data we can write.
   *
   * @return the maximum size of keys.
   */
  public int getMaxKeySize() {
    return maxKeySize;
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
   * Indicates if this environment was opened with
   * {@link EnvFlags#MDB_RDONLY_ENV}.
   *
   * @return true if read-only
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  public Dbi<T> openDbi(final String name, final DbiFlags... flags) {
    final byte[] nameBytes = name == null ? null : name.getBytes(UTF_8);
    return openDbi(nameBytes, flags);
  }

  /**
   * Open the {@link Dbi}.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  public Dbi<T> openDbi(final byte[] name, final DbiFlags... flags) {
    try (Txn<T> txn = readOnly ? txnRead() : txnWrite()) {
      final Dbi<T> dbi = new Dbi<>(this, txn, name, flags);
      txn.commit(); // even RO Txns require a commit to retain Dbi in Env
      return dbi;
    }
  }

  /**
   * Return statistics about this environment.
   *
   * @return an immutable statistics object.
   */
  public Stat stat() {
    if (closed) {
      throw new AlreadyClosedException();
    }
    final MDB_stat stat = new MDB_stat(RUNTIME);
    checkRc(LIB.mdb_env_stat(ptr, stat));
    return new Stat(
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
    if (closed) {
      throw new AlreadyClosedException();
    }
    return new Txn<>(this, parent, proxy, flags);
  }

  /**
   * Obtain a read-only transaction.
   *
   * @return a read-only transaction
   */
  public Txn<T> txnRead() {
    return txn(null, MDB_RDONLY_TXN);
  }

  /**
   * Obtain a read-write transaction.
   *
   * @return a read-write transaction
   */
  public Txn<T> txnWrite() {
    return txn(null);
  }

  Pointer pointer() {
    return ptr;
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
   * Builder for configuring and opening Env.
   *
   * @param <T> buffer type
   */
  public static final class Builder<T> {

    private long mapSize = 1_024 * 1_024;
    private int maxDbs = 1;
    private int maxReaders = 1;
    private boolean opened;
    private final BufferProxy<T> proxy;

    Builder(final BufferProxy<T> proxy) {
      requireNonNull(proxy);
      this.proxy = proxy;
    }

    /**
     * Opens the environment.
     *
     * @param path  file system destination
     * @param mode  Unix permissions to set on created files and semaphores
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    public Env<T> open(final File path, final int mode,
                       final EnvFlags... flags) {
      requireNonNull(path);
      if (opened) {
        throw new AlreadyOpenException();
      }
      opened = true;
      final PointerByReference envPtr = new PointerByReference();
      checkRc(LIB.mdb_env_create(envPtr));
      final Pointer ptr = envPtr.getValue();
      try {
        checkRc(LIB.mdb_env_set_mapsize(ptr, mapSize));
        checkRc(LIB.mdb_env_set_maxdbs(ptr, maxDbs));
        checkRc(LIB.mdb_env_set_maxreaders(ptr, maxReaders));
        final int flagsMask = mask(flags);
        final boolean readOnly = isSet(flagsMask, MDB_RDONLY_ENV);
        checkRc(LIB.mdb_env_open(ptr, path.getAbsolutePath(), flagsMask, mode));
        return new Env<>(proxy, ptr, readOnly); // NOPMD
      } catch (final LmdbNativeException e) {
        LIB.mdb_env_close(ptr);
        throw e;
      }
    }

    /**
     * Opens the environment with 0664 mode.
     *
     * @param path  file system destination
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    public Env<T> open(final File path, final EnvFlags... flags) {
      return open(path, 0664, flags); // NOPMD
    }

    /**
     * Sets the map size.
     *
     * @param mapSize new limit in bytes
     * @return the builder
     */
    public Builder<T> setMapSize(final long mapSize) {
      if (opened) {
        throw new AlreadyOpenException();
      }
      if (mapSize < 0) {
        throw new IllegalArgumentException("Negative value; overflow?");
      }
      this.mapSize = mapSize;
      return this;
    }

    /**
     * Sets the maximum number of databases (ie {@link Dbi}s permitted.
     *
     * @param dbs new limit
     * @return the builder
     */
    public Builder<T> setMaxDbs(final int dbs) {
      if (opened) {
        throw new AlreadyOpenException();
      }
      this.maxDbs = dbs;
      return this;
    }

    /**
     * Sets the maximum number of databases permitted.
     *
     * @param readers new limit
     * @return the builder
     */
    public Builder<T> setMaxReaders(final int readers) {
      if (opened) {
        throw new AlreadyOpenException();
      }
      this.maxReaders = readers;
      return this;
    }
  }

  /**
   * File is not a valid LMDB file.
   */
  public static final class FileInvalidException extends LmdbNativeException {

    static final int MDB_INVALID = -30_793;
    private static final long serialVersionUID = 1L;

    FileInvalidException() {
      super(MDB_INVALID, "File is not a valid LMDB file");
    }
  }

  /**
   * The specified copy destination is invalid.
   */
  public static final class InvalidCopyDestination extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     *
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

    static final int MDB_MAP_FULL = -30_792;
    private static final long serialVersionUID = 1L;

    MapFullException() {
      super(MDB_MAP_FULL, "Environment mapsize reached");
    }
  }

  /**
   * Environment maxreaders reached.
   */
  public static final class ReadersFullException extends LmdbNativeException {

    static final int MDB_READERS_FULL = -30_790;
    private static final long serialVersionUID = 1L;

    ReadersFullException() {
      super(MDB_READERS_FULL, "Environment maxreaders reached");
    }
  }

  /**
   * Environment version mismatch.
   */
  public static final class VersionMismatchException extends LmdbNativeException {

    static final int MDB_VERSION_MISMATCH = -30_794;
    private static final long serialVersionUID = 1L;

    VersionMismatchException() {
      super(MDB_VERSION_MISMATCH, "Environment version mismatch");
    }
  }

}
