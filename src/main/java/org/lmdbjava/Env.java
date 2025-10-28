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

import static java.lang.Boolean.getBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.ResultCodeMapper.checkRc;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import jnr.ffi.Pointer;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.PointerByReference;
import org.lmdbjava.Library.MDB_envinfo;
import org.lmdbjava.Library.MDB_stat;

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
  public static final Charset DEFAULT_NAME_CHARSET = StandardCharsets.UTF_8;

  /**
   * Indicates whether optional checks should be applied in LmdbJava. Optional checks are only
   * disabled in critical paths (see package-level JavaDocs). Non-critical paths have optional
   * checks performed at all times, regardless of this property.
   */
  public static final boolean SHOULD_CHECK = !getBoolean(DISABLE_CHECKS_PROP);

  private boolean closed;
  private final int maxKeySize;
  private final boolean noSubDir;
  private final BufferProxy<T> proxy;
  private final Pointer ptr;
  private final boolean readOnly;

  private Env(
      final BufferProxy<T> proxy,
      final Pointer ptr,
      final boolean readOnly,
      final boolean noSubDir) {
    this.proxy = proxy;
    this.readOnly = readOnly;
    this.noSubDir = noSubDir;
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
   * Opens an environment with a single default database in 0664 mode using the {@link
   * ByteBufferProxy#PROXY_OPTIMAL}.
   *
   * @param path  file system destination
   * @param size  size in megabytes
   * @param flags the flags for this new environment
   * @return env the environment (never null)
   */
  public static Env<ByteBuffer> open(final File path, final int size, final EnvFlags... flags) {
    return new Builder<>(PROXY_OPTIMAL)
        .setMapSize(size * 1_024L * 1_024L)
        .open(path, flags);
  }

  /**
   * Close the handle.
   *
   * <p>Will silently return if already closed or never opened.
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
   * <p>This function may be used to make a backup of an existing environment. No lockfile is
   * created, since it gets recreated at need.
   *
   * <p>If this environment was created using {@link EnvFlags#MDB_NOSUBDIR}, the destination path
   * must be a directory that exists but contains no files. If {@link EnvFlags#MDB_NOSUBDIR} was
   * used, the destination path must not exist, but it must be possible to create a file at the
   * provided path.
   *
   * <p>Note: This call can trigger significant file size growth if run in parallel with write
   * transactions, because it employs a read-only transaction. See long-lived transactions under
   * "Caveats" in the LMDB native documentation.
   *
   * @param path writable destination path as described above
   */
  public void copy(final File path) {
    copy(path, CopyFlagSet.EMPTY);
  }

  /**
   * Copies an LMDB environment to the specified destination path.
   *
   * <p>This function may be used to make a backup of an existing environment. No lockfile is
   * created, since it gets recreated at need.
   *
   * <p>If this environment was created using {@link EnvFlags#MDB_NOSUBDIR}, the destination path
   * must be a directory that exists but contains no files. If {@link EnvFlags#MDB_NOSUBDIR} was
   * used, the destination path must not exist, but it must be possible to create a file at the
   * provided path.
   *
   * <p>Note: This call can trigger significant file size growth if run in parallel with write
   * transactions, because it employs a read-only transaction. See long-lived transactions under
   * "Caveats" in the LMDB native documentation.
   *
   * @param path  writable destination path as described above
   * @param flags special options for this copy
   */
  public void copy(final File path, final CopyFlagSet flags) {
    requireNonNull(path);
    validatePath(path);
    checkRc(LIB.mdb_env_copy2(ptr, path.getAbsolutePath(), flags.getMask()));
  }

  /**
   * Obtain the DBI names.
   *
   * <p>This method is only compatible with {@link Env}s that use named databases. If an unnamed
   * {@link Dbi} is being used to store data, this method will attempt to return all such keys from
   * the unnamed database.
   *
   * <p>This method must not be called from concurrent threads.
   *
   * @return a list of DBI names (never null)
   */
  public List<byte[]> getDbiNames() {
    final List<byte[]> result = new ArrayList<>();
    final Dbi<T> names = openDbi((byte[]) null);
    try (Txn<T> txn = txnRead();
         Cursor<T> cursor = names.openCursor(txn)) {
      if (!cursor.first()) {
        return Collections.emptyList();
      }
      do {
        final byte[] name = proxy.getBytes(cursor.key());
        result.add(name);
      } while (cursor.next());
    }

    return result;
  }

  /**
   * Set the size of the data memory map.
   *
   * @param mapSize the new size, in bytes
   */
  public void setMapSize(final long mapSize) {
    checkRc(LIB.mdb_env_set_mapsize(ptr, mapSize));
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
   * Indicates if this environment was opened with {@link EnvFlags#MDB_RDONLY_ENV}.
   *
   * @return true if read-only
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Open (and optionally creates, if {@link DbiFlags#MDB_CREATE} is set)
   * a {@link Dbi} using a builder.
   *
   * @return A new builder instance for creating/opening a {@link Dbi}.
   */
  public DbiBuilder<T> buildDbi() {
    return new DbiBuilder<>(this, proxy, readOnly);
  }

  /**
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name and default {@link
   * Comparator} that is not invoked from native code.
   */
  @Deprecated()
  public Dbi<T> openDbi(final String name, final DbiFlags... flags) {
    final byte[] nameBytes = name == null ? null : name.getBytes(UTF_8);
    return openDbi(nameBytes, null, false, flags);
  }

  /**
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator for cursor start/stop key comparisons. If null, LMDB's
   *                   comparator will be used.
   * @param flags      to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name and associated {@link
   * Comparator} for use by {@link CursorIterable} when comparing start/stop keys.
   *
   * <p>It is very important that the passed comparator behaves in the same way as the comparator
   * LMDB uses for its insertion order (for the type of data that will be stored in the database),
   * or you fully understand the implications of them behaving differently. LMDB's comparator is
   * unsigned lexicographical, unless {@link DbiFlags#MDB_INTEGERKEY} is used.
   */
  @Deprecated()
  public Dbi<T> openDbi(final String name,
                        final Comparator<T> comparator,
                        final DbiFlags... flags) {
    final byte[] nameBytes = name == null ? null : name.getBytes(UTF_8);
    return openDbi(nameBytes, comparator, false, flags);
  }

  /**
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator for cursor start/stop key comparisons and optionally for
   *                   LMDB to call back to. If null, LMDB's comparator will be used.
   * @param nativeCb   whether LMDB native code calls back to the Java comparator
   * @param flags      to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name and associated {@link
   * Comparator}. The comparator will be used by {@link CursorIterable} when comparing start/stop
   * keys as a minimum. If nativeCb is {@code true}, this comparator will also be called by LMDB to
   * determine insertion/iteration order. Calling back to a java comparator may significantly impact
   * performance.
   */
  @Deprecated()
  public Dbi<T> openDbi(final String name,
                        final Comparator<T> comparator,
                        final boolean nativeCb,
                        final DbiFlags... flags) {
    final byte[] nameBytes = name == null ? null : name.getBytes(UTF_8);
    return openDbi(nameBytes, comparator, nativeCb, flags);
  }

  /**
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * <hr>
   * Convenience method that opens a {@link Dbi} with a default {@link Comparator} that is not
   * invoked from native code.
   */
  @Deprecated()
  public Dbi<T> openDbi(final byte[] name,
                        final DbiFlags... flags) {
    return openDbi(name, null, false, flags);
  }

  /**
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param flags      to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * <hr>
   * Convenience method that opens a {@link Dbi} with an associated {@link Comparator} that is not
   * invoked from native code.
   */
  @Deprecated()
  public Dbi<T> openDbi(final byte[] name,
                        final Comparator<T> comparator,
                        final DbiFlags... flags) {
    return openDbi(name, comparator, false, flags);
  }

  /**
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param nativeCb   whether native code calls back to the Java comparator
   * @param flags      to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * <hr>
   * Convenience method that opens a {@link Dbi} with an associated {@link Comparator} that may be
   * invoked from native code if specified.
   *
   * <p>This method will automatically commit the private transaction before returning. This ensures
   * the <code>Dbi</code> is available in the <code>Env</code>.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final byte[] name,
      final Comparator<T> comparator,
      final boolean nativeCb,
      final DbiFlags... flags) {
    try (Txn<T> txn = readOnly ? txnRead() : txnWrite()) {
      final Dbi<T> dbi = openDbi(txn, name, comparator, nativeCb, flags);
      txn.commit(); // even RO Txns require a commit to retain Dbi in Env
      return dbi;
    }
  }

  /**
   * @param txn        transaction to use (required; not closed)
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param nativeCb   whether native LMDB code should call back to the Java comparator
   * @param flags      to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#buildDbi()}
   * Open the {@link Dbi} using the passed {@link Txn}.
   *
   * <p>The caller must commit the transaction after this method returns in order to retain the
   * <code>Dbi</code> in the <code>Env</code>.
   *
   * <p>A {@link Comparator} may be provided when calling this method. Such comparator is primarily
   * used by {@link CursorIterable} instances. A secondary (but uncommon) use of the comparator is
   * to act as a callback from the native library if <code>nativeCb</code> is <code>true</code>.
   * This is usually avoided due to the overhead of native code calling back into Java. It is
   * instead highly recommended to set the correct {@link DbiFlags} to allow the native library to
   * correctly order the intended keys.
   *
   * <p>A default comparator will be provided if <code>null</code> is passed as the comparator. If a
   * custom comparator is provided, it must strictly match the lexicographical order of keys in the
   * native LMDB database.
   *
   * <p>This method (and its overloaded convenience variants) must not be called from concurrent
   * threads.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final Txn<T> txn,
      final byte[] name,
      final Comparator<T> comparator,
      final boolean nativeCb,
      final DbiFlags... flags) {

    if (nativeCb && comparator == null) {
      throw new IllegalArgumentException("Is nativeCb is true, you must supply a comparator");
    }
    return new Dbi<>(this, txn, name, comparator, nativeCb, proxy, DbiFlagSet.of(flags));
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
   * @param force force a synchronous flush (otherwise if the environment has the MDB_NOSYNC flag
   *              set the flushes will be omitted, and with MDB_MAPASYNC they will be asynchronous)
   */
  public void sync(final boolean force) {
    if (closed) {
      throw new AlreadyClosedException();
    }
    final int f = force ? 1 : 0;
    checkRc(LIB.mdb_env_sync(ptr, f));
  }

  /**
   * @param parent parent transaction (may be null if no parent)
   * @param flags  applicable flags (eg for a reusable, read-only transaction)
   * @return a transaction (never null)
   * @deprecated Instead use {@link Env#txn(Txn, TxnFlagSet)}
   * <p>
   * Obtain a transaction with the requested parent and flags.
   */
  @Deprecated
  public Txn<T> txn(final Txn<T> parent, final TxnFlags... flags) {
    checkNotClosed();
    return new Txn<>(this, parent, proxy, TxnFlagSet.of(flags));
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @return a transaction (never null)
   */
  public Txn<T> txn(final Txn<T> parent) {
    checkNotClosed();
    return new Txn<>(this, parent, proxy, TxnFlagSet.EMPTY);
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @param flag   applicable flag (eg for a reusable, read-only transaction)
   * @return a transaction (never null)
   */
  public Txn<T> txn(final Txn<T> parent, final TxnFlags flag) {
    checkNotClosed();
    return new Txn<>(this, parent, proxy, flag);
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @param flags  applicable flags (e.g. for a reusable, read-only transaction).
   *               If the set of flags is used frequently it is recommended to hold
   *               a static instance of the {@link TxnFlagSet} for re-use.
   * @return a transaction (never null)
   */
  public Txn<T> txn(final Txn<T> parent, final TxnFlagSet flags) {
    checkNotClosed();
    return new Txn<>(this, parent, proxy, flags);
  }

  /**
   * Obtain a read-only transaction.
   *
   * @return a read-only transaction
   */
  public Txn<T> txnRead() {
    checkNotClosed();
    return new Txn<>(this, null, proxy, TxnFlags.MDB_RDONLY_TXN);
  }

  /**
   * Obtain a read-write transaction.
   *
   * @return a read-write transaction
   */
  public Txn<T> txnWrite() {
    checkNotClosed();
    return new Txn<>(this, null, proxy, TxnFlagSet.EMPTY);
  }

  Pointer pointer() {
    return ptr;
  }

  void checkNotClosed() {
    if (closed) {
      throw new AlreadyClosedException();
    }
  }

  private void validateDirectoryEmpty(final File path) {
    if (!path.exists()) {
      throw new InvalidCopyDestination("Path does not exist");
    }
    if (!path.isDirectory()) {
      throw new InvalidCopyDestination("Path must be a directory");
    }
    final String[] files = path.list();
    if (files != null && files.length > 0) {
      throw new InvalidCopyDestination("Path must contain no files");
    }
  }

  private void validatePath(final File path) {
    if (noSubDir) {
      if (path.exists()) {
        throw new InvalidCopyDestination("Path must not exist for MDB_NOSUBDIR");
      }
      return;
    }
    validateDirectoryEmpty(path);
  }

  /**
   * Check for stale entries in the reader lock table.
   *
   * @return 0 on success, non-zero on failure
   */
  public int readerCheck() {
    final IntByReference resultPtr = new IntByReference();
    checkRc(LIB.mdb_reader_check(ptr, resultPtr));
    return resultPtr.intValue();
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

    static final int MAX_READERS_DEFAULT = 126;
    private long mapSize = 1_024 * 1_024;
    private int maxDbs = 1;
    private int maxReaders = MAX_READERS_DEFAULT;
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
    public Env<T> open(final File path, final int mode, final EnvFlags... flags) {
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
        final boolean noSubDir = isSet(flagsMask, MDB_NOSUBDIR);
        checkRc(LIB.mdb_env_open(ptr, path.getAbsolutePath(), flagsMask, mode));
        return new Env<>(proxy, ptr, readOnly, noSubDir);
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
      return open(path, 0664, flags);
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
