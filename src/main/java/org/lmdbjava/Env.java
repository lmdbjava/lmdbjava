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
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.ResultCodeMapper.checkRc;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
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

  /** Java system property name that can be set to disable optional checks. */
  public static final String DISABLE_CHECKS_PROP = "lmdbjava.disable.checks";

  /**
   * The default {@link Charset} used to convert DB names from a byte[] to a String or to encode a
   * String as a byte[]. Only used if not explicit {@link Charset} is provided.
   */
  public static final Charset DEFAULT_NAME_CHARSET = StandardCharsets.UTF_8;

  /**
   * Indicates whether optional checks should be applied in LmdbJava. Optional checks are only
   * disabled in critical paths (see package-level JavaDocs). Non-critical paths have optional
   * checks performed at all times, regardless of this property.
   */
  public static final boolean SHOULD_CHECK = !getBoolean(DISABLE_CHECKS_PROP);

  // volatile: close() may run on a different thread than the readers that call checkNotClosed().
  // Without it there is no happens-before between the write here and those reads, so a reader could
  // indefinitely observe a stale false (the JIT may even hoist the check out of a hot loop) and
  // proceed into a native call on a freed env. This does NOT make close() atomic w.r.t. an
  // in-flight
  // txnRead()/txnWrite() (the check-then-mdb_txn_begin window in those methods remains); it removes
  // the pure visibility bug and turns more of those races into a clean AlreadyClosedException
  // rather
  // than a JVM crash. See close() for the full lifecycle contract.
  private volatile boolean closed;

  // Opt-in "safe close" (see Builder#setSafeClose). When enabled, live transactions are tracked so
  // close(Duration) can drain in-flight readers before unmapping instead of risking the JVM crash
  // documented on close(). All three fields are inert unless safeClose is true, so the default hot
  // path is byte-for-byte unchanged: liveTxns is null and never touched, and the tracking branches
  // are guarded by the final safeClose flag.
  private final boolean safeClose;
  private volatile boolean closing;
  private final AtomicInteger liveTxns;

  private final int maxKeySize;
  private final boolean noSubDir;
  private final BufferProxy<T> proxy;
  private final Pointer ptr;
  private final boolean readOnly;
  private final Path path;
  private final EnvFlagSet envFlagSet;

  private Env(
      final BufferProxy<T> proxy,
      final Pointer ptr,
      final boolean readOnly,
      final boolean noSubDir,
      final Path path,
      final EnvFlagSet envFlagSet,
      final boolean safeClose) {
    this.proxy = proxy;
    this.readOnly = readOnly;
    this.noSubDir = noSubDir;
    this.ptr = ptr;
    // cache max key size to avoid further JNI calls
    this.maxKeySize = LIB.mdb_env_get_maxkeysize(ptr);
    this.path = path;
    this.envFlagSet = envFlagSet;
    this.safeClose = safeClose;
    this.liveTxns = safeClose ? new AtomicInteger() : null;
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
   * @param <T> buffer type
   * @param proxy the proxy to use (required)
   * @return the environment (never null)
   */
  public static <T> Builder<T> create(final BufferProxy<T> proxy) {
    return new Builder<>(proxy);
  }

  /**
   * @param path file system destination
   * @param size size in megabytes
   * @param flags the flags for this new environment
   * @return env the environment (never null)
   * @deprecated Instead use {@link Env#create()} or {@link Env#create(BufferProxy)}
   *     <p>Opens an environment with a single default database in 0664 mode using the {@link
   *     ByteBufferProxy#PROXY_OPTIMAL}.
   */
  @Deprecated
  public static Env<ByteBuffer> open(final File path, final int size, final EnvFlags... flags) {
    return new Builder<>(PROXY_OPTIMAL).setMapSize(size, ByteUnit.MEBIBYTES).open(path, flags);
  }

  /**
   * Close the handle.
   *
   * <p>Will silently return if already closed or never opened.
   *
   * <p><strong>Thread-safety / lifecycle contract.</strong> This method is <em>not</em>
   * synchronized and (consistent with the package-level policy that LmdbJava provides no
   * concurrency guarantees) it does not coordinate with other threads. Before and during this call
   * the caller MUST ensure that:
   *
   * <ul>
   *   <li>every {@link Txn}, {@link Cursor} and {@link Dbi} obtained from this environment has
   *       already been closed; and
   *   <li>no other thread is executing <em>any</em> operation on this environment or on a handle
   *       derived from it — including {@link #txnRead()} / {@link #txnWrite()} and reads such as
   *       {@code Dbi.get}.
   * </ul>
   *
   * <p>Violating this contract is <strong>undefined behaviour that can crash the whole JVM</strong>
   * ({@code SIGSEGV} on Linux/macOS, {@code EXCEPTION_ACCESS_VIOLATION 0xC0000005} on Windows); it
   * does <em>not</em> raise a Java exception. The underlying {@code mdb_env_close} unmaps the
   * memory map, so a transaction still being started or used on another thread then dereferences
   * freed memory — typically observed as a native crash in {@code mdb_txn_renew0} / {@code
   * mdb_txn_begin}.
   *
   * <p>If you must close an environment while reader threads may still be active, serialise the
   * close against those readers in application code: e.g. a read/write lock where each reader holds
   * the read lock for the entire duration of its transaction and {@code close()} holds the write
   * lock, so the map is never unmapped while a read is in flight.
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
   * Close the handle, first draining any in-flight transactions ("safe close").
   *
   * <p>This is the opt-in, thread-safe counterpart to {@link #close()} and requires the environment
   * to have been built with {@link Builder#setSafeClose(boolean) setSafeClose(true)} (otherwise an
   * {@link IllegalStateException} is thrown). Unlike {@link #close()} it will <em>not</em> unmap
   * the memory map while a transaction is still live, so it does not risk the JVM crash described
   * on {@link #close()}.
   *
   * <p>On entry it stops new {@link #txnRead()} / {@link #txnWrite()} calls (they throw {@link
   * AlreadyClosedException}), then waits up to {@code timeout} for every transaction obtained from
   * this environment to be closed before calling the native close. If the timeout elapses with
   * transactions still open it throws {@link CloseTimeoutException} and does <em>not</em> unmap
   * (leaked or stuck transactions are a caller bug; forcing the unmap would reintroduce the crash).
   *
   * <p>Idempotent: returns immediately if the environment is already closed. This method only
   * tracks transactions created <em>after</em> the environment was opened with safe close enabled;
   * it cannot police direct native misuse or handles shared across processes.
   *
   * @param timeout maximum time to wait for in-flight transactions to drain
   * @throws IllegalStateException if this environment was not built with safe close enabled
   * @throws CloseTimeoutException if transactions remain open after {@code timeout}
   */
  public void close(final Duration timeout) {
    requireNonNull(timeout);
    if (!safeClose) {
      throw new IllegalStateException(
          "close(Duration) requires Env.Builder.setSafeClose(true); use close() otherwise");
    }
    if (closed) {
      return;
    }
    // Publish "closing" before reading the live count. A reader increments liveTxns before reading
    // closing (see beforeTxnBeginIfTracked); with both being volatile/atomic this handshake ensures
    // that if the drain below observes zero live txns, any concurrent reader will observe closing
    // and back out before it starts a native transaction. So the map is never unmapped under a live
    // or about-to-start read.
    closing = true;
    final long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (liveTxns.get() > 0) {
      if (System.nanoTime() - deadlineNanos >= 0L) {
        throw new CloseTimeoutException(liveTxns.get());
      }
      LockSupport.parkNanos(500_000L); // 0.5 ms; close is rare, so a short poll is fine
    }
    closed = true;
    LIB.mdb_env_close(ptr);
  }

  /**
   * Registers a soon-to-begin transaction when safe close is enabled, and returns whether tracking
   * happened so the caller can balance the count if {@code mdb_txn_begin} then fails. Must be
   * called before the native transaction start so a concurrent {@link #close(Duration)} cannot
   * unmap between the check and the begin.
   */
  private boolean beforeTxnBeginIfTracked() {
    if (!safeClose) {
      return false;
    }
    liveTxns.incrementAndGet();
    if (closing || closed) {
      liveTxns.decrementAndGet();
      throw new AlreadyClosedException();
    }
    return true;
  }

  /** Balances {@link #beforeTxnBeginIfTracked()} when a tracked transaction is closed. */
  void afterTxnClosed() {
    if (safeClose) {
      liveTxns.decrementAndGet();
    }
  }

  /**
   * Indicates whether this environment was built with the opt-in safe close enabled.
   *
   * @return true if {@link Builder#setSafeClose(boolean)} was set
   */
  public boolean isSafeClose() {
    return safeClose;
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
   * @param flags special options for this copy
   * @deprecated Use {@link Env#copy(Path, CopyFlagSet)}
   */
  @Deprecated
  public void copy(final File path, final CopyFlags... flags) {
    requireNonNull(path);
    copy(path.toPath(), CopyFlagSet.of(flags));
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
  public void copy(final Path path) {
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
   * @param path writable destination path as described above
   * @param flags special options for this copy
   */
  public void copy(final Path path, final CopyFlagSet flags) {
    requireNonNull(path);
    requireNonNull(flags);
    validatePath(path);
    checkRc(LIB.mdb_env_copy2(ptr, path.toAbsolutePath().toString(), flags.getMask()));
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
    // The unnamed DB is special so the names of the named DBs are held as keys in it.
    try (final Txn<T> readTxn = txnRead()) {
      final Dbi<T> unnamedDb = new Dbi<>(this, readTxn, null, proxy, DbiFlagSet.EMPTY);
      try (final Cursor<T> cursor = unnamedDb.openCursor(readTxn)) {
        if (!cursor.first()) {
          return Collections.emptyList();
        }
        do {
          final byte[] name = proxy.getBytes(cursor.key());
          result.add(name);
        } while (cursor.next());
      }
    }
    return Collections.unmodifiableList(result);
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
  public List<String> getDbiNames(final Charset charset) {
    final List<byte[]> dbiNames = getDbiNames();
    return dbiNames.stream()
        .map(nameBytes -> Dbi.getNameAsString(nameBytes, charset))
        .collect(Collectors.toList());
  }

  /**
   * Set the size of the data memory map.
   *
   * @param mapSize the new size, in bytes
   */
  public void setMapSize(final long mapSize) {
    if (mapSize < 0) {
      throw new IllegalArgumentException("Negative value; overflow?");
    }
    checkRc(LIB.mdb_env_set_mapsize(ptr, mapSize));
  }

  /**
   * Set the size of the data memory map.
   *
   * @param mapSize new map size in the units of byteUnit.
   * @param byteUnit The unit that mapSize is in.
   */
  public void setMapSize(final long mapSize, final ByteUnit byteUnit) {
    requireNonNull(byteUnit);
    setMapSize(byteUnit.toBytes(mapSize));
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
   * Returns a builder for creating and opening a {@link Dbi} instance in this {@link Env}.
   *
   * <p>The flag {@link DbiFlags#MDB_CREATE} needs to be set on the builder if you need to create a
   * new database before opening it.
   *
   * @return A new builder instance for creating/opening a {@link Dbi}.
   */
  public DbiBuilder<T> createDbi() {
    return new DbiBuilder<>(this, proxy, readOnly);
  }

  /**
   * @param name name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Convenience method that opens a {@link Dbi} with a UTF-8 database name and default
   *     {@link Comparator} that is not invoked from native code.
   */
  @Deprecated()
  public Dbi<T> openDbi(final String name, final DbiFlags... flags) {
    return openDbi(Dbi.getNameBytes(name), null, false, flags);
  }

  /**
   * @param name name of the database (or null if no name is required)
   * @param comparator custom comparator for cursor start/stop key comparisons. If null, LMDB's
   *     comparator will be used.
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Convenience method that opens a {@link Dbi} with a UTF-8 database name and associated
   *     {@link Comparator} for use by {@link CursorIterable} when comparing start/stop keys.
   *     <p>It is very important that the passed comparator behaves in the same way as the
   *     comparator LMDB uses for its insertion order (for the type of data that will be stored in
   *     the database), or you fully understand the implications of them behaving differently.
   *     LMDB's comparator is unsigned lexicographical, unless {@link DbiFlags#MDB_INTEGERKEY} is
   *     used.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final String name, final Comparator<T> comparator, final DbiFlags... flags) {
    return openDbi(Dbi.getNameBytes(name), comparator, false, flags);
  }

  /**
   * @param name name of the database (or null if no name is required)
   * @param comparator custom comparator for cursor start/stop key comparisons and optionally for
   *     LMDB to call back to. If null, LMDB's comparator will be used.
   * @param nativeCb whether LMDB native code calls back to the Java comparator
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Convenience method that opens a {@link Dbi} with a UTF-8 database name and associated
   *     {@link Comparator}. The comparator will be used by {@link CursorIterable} when comparing
   *     start/stop keys as a minimum. If nativeCb is {@code true}, this comparator will also be
   *     called by LMDB to determine insertion/iteration order. Calling back to a java comparator
   *     may significantly impact performance.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final String name,
      final Comparator<T> comparator,
      final boolean nativeCb,
      final DbiFlags... flags) {
    return openDbi(Dbi.getNameBytes(name), comparator, nativeCb, flags);
  }

  /**
   * @param name name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Convenience method that opens a {@link Dbi} with a default {@link Comparator} that is
   *     not invoked from native code.
   */
  @Deprecated()
  public Dbi<T> openDbi(final byte[] name, final DbiFlags... flags) {
    return openDbi(name, null, false, flags);
  }

  /**
   * @param name name of the database (or null if no name is required)
   * @param comparator custom iterator comparator (or null to use LMDB default)
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Convenience method that opens a {@link Dbi} with an associated {@link Comparator} that
   *     is not invoked from native code.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final byte[] name, final Comparator<T> comparator, final DbiFlags... flags) {
    return openDbi(name, comparator, false, flags);
  }

  /**
   * @param name name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param nativeCb whether native code calls back to the Java comparator
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Convenience method that opens a {@link Dbi} with an associated {@link Comparator} that
   *     may be invoked from native code if specified.
   *     <p>This method will automatically commit the private transaction before returning. This
   *     ensures the <code>Dbi</code> is available in the <code>Env</code>.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final byte[] name,
      final Comparator<T> comparator,
      final boolean nativeCb,
      final DbiFlags... flags) {
    try (Txn<T> txn = readOnly ? txnRead() : txnWrite()) {
      final Dbi<T> dbi =
          new Dbi<>(this, txn, name, comparator, nativeCb, proxy, DbiFlagSet.of(flags));
      txn.commit(); // even RO Txns require a commit to retain Dbi in Env
      return dbi;
    }
  }

  /**
   * @param txn transaction to use (required; not closed)
   * @param name name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param nativeCb whether native LMDB code should call back to the Java comparator
   * @param flags to open the database with
   * @return a database that is ready to use
   * @deprecated Instead use {@link Env#createDbi()}
   *     <p>Open the {@link Dbi} using the passed {@link Txn}.
   *     <p>The caller must commit the transaction after this method returns in order to retain the
   *     <code>Dbi</code> in the <code>Env</code>.
   *     <p>A {@link Comparator} may be provided when calling this method. Such comparator is
   *     primarily used by {@link CursorIterable} instances. A secondary (but uncommon) use of the
   *     comparator is to act as a callback from the native library if <code>nativeCb</code> is
   *     <code>true</code>. This is usually avoided due to the overhead of native code calling back
   *     into Java. It is instead highly recommended to set the correct {@link DbiFlags} to allow
   *     the native library to correctly order the intended keys.
   *     <p>A default comparator will be provided if <code>null</code> is passed as the comparator.
   *     If a custom comparator is provided, it must strictly match the lexicographical order of
   *     keys in the native LMDB database.
   *     <p>This method (and its overloaded convenience variants) must not be called from concurrent
   *     threads.
   */
  @Deprecated()
  public Dbi<T> openDbi(
      final Txn<T> txn,
      final byte[] name,
      final Comparator<T> comparator,
      final boolean nativeCb,
      final DbiFlags... flags) {
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
   *     set the flushes will be omitted, and with MDB_MAPASYNC they will be asynchronous)
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
   * @param flags applicable flags (eg for a reusable, read-only transaction)
   * @return a transaction (never null)
   * @deprecated Instead use {@link Env#txn(Txn, TxnFlagSet)}
   *     <p>Obtain a transaction with the requested parent and flags.
   */
  @Deprecated
  public Txn<T> txn(final Txn<T> parent, final TxnFlags... flags) {
    checkNotClosed();
    final boolean tracked = beforeTxnBeginIfTracked();
    try {
      return new Txn<>(this, parent, proxy, TxnFlagSet.of(flags));
    } catch (final RuntimeException e) {
      if (tracked) {
        liveTxns.decrementAndGet();
      }
      throw e;
    }
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @return a transaction (never null)
   */
  public Txn<T> txn(final Txn<T> parent) {
    checkNotClosed();
    final boolean tracked = beforeTxnBeginIfTracked();
    try {
      return new Txn<>(this, parent, proxy, TxnFlagSet.EMPTY);
    } catch (final RuntimeException e) {
      if (tracked) {
        liveTxns.decrementAndGet();
      }
      throw e;
    }
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @param flags applicable flags (e.g. for a reusable, read-only transaction). If the set of flags
   *     is used frequently it is recommended to hold a static instance of the {@link TxnFlagSet}
   *     for re-use.
   * @return a transaction (never null)
   */
  public Txn<T> txn(final Txn<T> parent, final TxnFlagSet flags) {
    checkNotClosed();
    final boolean tracked = beforeTxnBeginIfTracked();
    try {
      return new Txn<>(this, parent, proxy, flags);
    } catch (final RuntimeException e) {
      if (tracked) {
        liveTxns.decrementAndGet();
      }
      throw e;
    }
  }

  /**
   * Obtain a read-only transaction.
   *
   * <p>Must not race a concurrent {@link #close()} on another thread: the closed-check and the
   * native transaction start are not atomic, so a close occurring between them can crash the JVM
   * (see {@link #close()}).
   *
   * @return a read-only transaction
   * @throws Env.AlreadyClosedException if this environment has already been closed
   */
  public Txn<T> txnRead() {
    checkNotClosed();
    final boolean tracked = beforeTxnBeginIfTracked();
    try {
      return new Txn<>(this, null, proxy, TxnFlags.MDB_RDONLY_TXN);
    } catch (final RuntimeException e) {
      if (tracked) {
        liveTxns.decrementAndGet();
      }
      throw e;
    }
  }

  /**
   * Obtain a read-write transaction.
   *
   * <p>Must not race a concurrent {@link #close()} on another thread: the closed-check and the
   * native transaction start are not atomic, so a close occurring between them can crash the JVM
   * (see {@link #close()}).
   *
   * @return a read-write transaction
   * @throws Env.AlreadyClosedException if this environment has already been closed
   */
  public Txn<T> txnWrite() {
    checkNotClosed();
    final boolean tracked = beforeTxnBeginIfTracked();
    try {
      return new Txn<>(this, null, proxy, TxnFlagSet.EMPTY);
    } catch (final RuntimeException e) {
      if (tracked) {
        liveTxns.decrementAndGet();
      }
      throw e;
    }
  }

  Pointer pointer() {
    return ptr;
  }

  void checkNotClosed() {
    if (closed) {
      throw new AlreadyClosedException();
    }
  }

  private void validateDirectoryEmpty(final Path path) {
    if (!Files.exists(path)) {
      throw new InvalidCopyDestination("Path does not exist");
    }
    if (!Files.isDirectory(path)) {
      throw new InvalidCopyDestination("Path must be a directory");
    }
    final long fileCount = FileUtil.count(path);
    if (fileCount > 0) {
      throw new InvalidCopyDestination("Path must contain no files");
    }
  }

  private void validatePath(final Path path) {
    if (noSubDir) {
      if (Files.exists(path)) {
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

  /** For testing use. */
  EnvFlagSet getEnvFlagSet() {
    return envFlagSet;
  }

  @Override
  public String toString() {
    return "Env{"
        + "closed="
        + closed
        + ", maxKeySize="
        + maxKeySize
        + ", noSubDir="
        + noSubDir
        + ", readOnly="
        + readOnly
        + ", path="
        + path
        + ", envFlagSet="
        + envFlagSet
        + '}';
  }

  /** Object has already been closed and the operation is therefore prohibited. */
  public static final class AlreadyClosedException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public AlreadyClosedException() {
      super("Environment has already been closed");
    }
  }

  /**
   * {@link Env#close(Duration)} timed out because transactions were still open. The environment has
   * <em>not</em> been closed (its memory map is still mapped); the caller should ensure the
   * offending transactions are closed and retry, rather than forcing an unsafe {@link Env#close()}.
   */
  public static final class CloseTimeoutException extends LmdbException {

    private static final long serialVersionUID = 1L;

    CloseTimeoutException(final int openTransactions) {
      super(
          "close(Duration) timed out while "
              + openTransactions
              + " transaction(s) were still open");
    }
  }

  /** Object has already been opened and the operation is therefore prohibited. */
  public static final class AlreadyOpenException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
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
    static final long MAP_SIZE_DEFAULT = ByteUnit.MEBIBYTES.toBytes(1);
    static final int POSIX_MODE_DEFAULT = 0664;

    private long mapSize = MAP_SIZE_DEFAULT;
    private int maxDbs = 1;
    private int maxReaders = MAX_READERS_DEFAULT;
    private boolean opened;
    private final BufferProxy<T> proxy;
    private int mode = POSIX_MODE_DEFAULT;
    private boolean safeClose;
    private final AbstractFlagSet.Builder<EnvFlags, EnvFlagSet> flagSetBuilder =
        EnvFlagSet.builder();

    Builder(final BufferProxy<T> proxy) {
      requireNonNull(proxy);
      this.proxy = proxy;
    }

    /**
     * Opens the environment.
     *
     * @param path file system destination
     * @param mode Unix permissions to set on created files and semaphores
     * @param flags the flags for this new environment
     * @return an environment ready for use
     * @deprecated Instead use {@link Builder#open(Path)}, {@link Builder#setFilePermissions(int)}
     *     and {@link Builder#setEnvFlags(EnvFlags...)}.
     */
    @Deprecated
    public Env<T> open(final File path, final int mode, final EnvFlags... flags) {
      setFilePermissions(mode);
      setEnvFlags(flags);
      return open(requireNonNull(path).toPath());
    }

    /**
     * Opens the environment.
     *
     * @param path file system destination
     * @return an environment ready for use
     * @deprecated Instead use {@link Builder#open(Path)}
     */
    @Deprecated
    public Env<T> open(final File path) {
      return open(requireNonNull(path).toPath());
    }

    /**
     * Opens the environment with 0664 mode.
     *
     * @param path file system destination
     * @param flags the flags for this new environment
     * @return an environment ready for use
     * @deprecated Instead use {@link Builder#open(Path)} and {@link
     *     Builder#setEnvFlags(EnvFlags...)}.
     */
    @Deprecated
    public Env<T> open(final File path, final EnvFlags... flags) {
      setEnvFlags(flags);
      return open(requireNonNull(path).toPath());
    }

    /**
     * Opens the environment.
     *
     * @param path file system destination
     * @return an environment ready for use
     */
    public Env<T> open(final Path path) {
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
        final EnvFlagSet flags = flagSetBuilder.build();
        final boolean readOnly = flags.isSet(MDB_RDONLY_ENV);
        final boolean noSubDir = flags.isSet(MDB_NOSUBDIR);
        checkRc(LIB.mdb_env_open(ptr, path.toAbsolutePath().toString(), flags.getMask(), mode));
        return new Env<>(proxy, ptr, readOnly, noSubDir, path, flags, safeClose);
      } catch (final LmdbNativeException e) {
        LIB.mdb_env_close(ptr);
        throw e;
      }
    }

    /**
     * Sets the map size in bytes.
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
     * Sets the map size in the supplied unit.
     *
     * @param mapSize new map size in the units of byteUnit.
     * @param byteUnit The unit that mapSize is in.
     * @return the builder
     */
    public Builder<T> setMapSize(final long mapSize, final ByteUnit byteUnit) {
      requireNonNull(byteUnit);
      if (mapSize < 0) {
        throw new IllegalArgumentException("Negative value; overflow?");
      }
      return setMapSize(byteUnit.toBytes(mapSize));
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

    /**
     * Enables the opt-in "safe close" for the resulting {@link Env}.
     *
     * <p>When enabled, the environment tracks its live transactions so {@link Env#close(Duration)}
     * can drain in-flight readers before unmapping, instead of risking the JVM crash described on
     * {@link Env#close()}. This adds a small amount of bookkeeping on transaction start/close; it
     * is <strong>disabled by default</strong> so applications that already manage their own
     * threading (the common low-latency case) pay nothing. It does not change the behaviour of the
     * no-arg {@link Env#close()}.
     *
     * @param safeClose true to enable transaction tracking and {@link Env#close(Duration)}
     * @return the builder
     */
    public Builder<T> setSafeClose(final boolean safeClose) {
      if (opened) {
        throw new AlreadyOpenException();
      }
      this.safeClose = safeClose;
      return this;
    }

    /**
     * Sets the Unix file permissions to use on created files and semaphores, e.g. {@code 0664}. If
     * this method is not called, the default of {@code 0664} will be used.
     *
     * @param mode Unix permissions to set on created files and semaphores
     * @return the builder
     */
    public Builder<T> setFilePermissions(final int mode) {
      if (opened) {
        throw new AlreadyOpenException();
      }
      this.mode = mode;
      return this;
    }

    /**
     * Sets all the flags used to open this {@link Env}.
     *
     * @param envFlags The flags to use. Clears any existing flags. A null value results in no flags
     *     being set.
     * @return this builder instance.
     */
    public Builder<T> setEnvFlags(final Collection<EnvFlags> envFlags) {
      flagSetBuilder.clear();
      if (envFlags != null) {
        envFlags.stream().filter(Objects::nonNull).forEach(flagSetBuilder::addFlag);
      }
      return this;
    }

    /**
     * Sets all the flags used to open this {@link Env}.
     *
     * @param envFlags The flags to use. Clears any existing flags. A null value results in no flags
     *     being set.
     * @return this builder instance.
     */
    public Builder<T> setEnvFlags(final EnvFlags... envFlags) {
      flagSetBuilder.clear();
      if (envFlags != null) {
        Arrays.stream(envFlags).filter(Objects::nonNull).forEach(this.flagSetBuilder::addFlag);
      }
      return this;
    }

    /**
     * Sets all the flags used to open this {@link Env}.
     *
     * @param envFlagSet The flags to use. Clears any existing flags. A null value results in no
     *     flags being set.
     * @return this builder instance.
     */
    public Builder<T> setEnvFlags(final EnvFlagSet envFlagSet) {
      flagSetBuilder.clear();
      if (envFlagSet != null) {
        this.flagSetBuilder.setFlags(envFlagSet.getFlags());
      }
      return this;
    }

    /**
     * Adds a single {@link EnvFlags} to any existing flags.
     *
     * @param envFlag The flag to add to any existing flags. A null value is a no-op.
     * @return this builder instance.
     */
    public Builder<T> addEnvFlag(final EnvFlags envFlag) {
      this.flagSetBuilder.addFlag(envFlag);
      return this;
    }

    /**
     * Adds the contents of an {@link EnvFlagSet} to any existing flags.
     *
     * @param envFlagSet The set of flags to add to any existing flags. A null value is a no-op.
     * @return this builder instance.
     */
    public Builder<T> addEnvFlags(final EnvFlagSet envFlagSet) {
      if (envFlagSet != null) {
        flagSetBuilder.addFlags(envFlagSet.getFlags());
      }
      return this;
    }

    /**
     * Adds a {@link Collection} of {@link EnvFlags} to any existing flags.
     *
     * @param envFlags The {@link Collection} of flags to add to any existing flags. A null value is
     *     a no-op.
     * @return this builder instance.
     */
    public Builder<T> addEnvFlags(final Collection<EnvFlags> envFlags) {
      if (envFlags != null) {
        flagSetBuilder.addFlags(envFlags);
      }
      return this;
    }
  }

  /** File is not a valid LMDB file. */
  public static final class FileInvalidException extends LmdbNativeException {

    static final int MDB_INVALID = -30_793;
    private static final long serialVersionUID = 1L;

    FileInvalidException() {
      super(MDB_INVALID, "File is not a valid LMDB file");
    }
  }

  /** The specified copy destination is invalid. */
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

  /** Environment mapsize reached. */
  public static final class MapFullException extends LmdbNativeException {

    static final int MDB_MAP_FULL = -30_792;
    private static final long serialVersionUID = 1L;

    MapFullException() {
      super(MDB_MAP_FULL, "Environment mapsize reached");
    }
  }

  /** Environment maxreaders reached. */
  public static final class ReadersFullException extends LmdbNativeException {

    static final int MDB_READERS_FULL = -30_790;
    private static final long serialVersionUID = 1L;

    ReadersFullException() {
      super(MDB_READERS_FULL, "Environment maxreaders reached");
    }
  }

  /** Environment version mismatch. */
  public static final class VersionMismatchException extends LmdbNativeException {

    static final int MDB_VERSION_MISMATCH = -30_794;
    private static final long serialVersionUID = 1L;

    VersionMismatchException() {
      super(MDB_VERSION_MISMATCH, "Environment version mismatch");
    }
  }
}
