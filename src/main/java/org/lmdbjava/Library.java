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

import static java.io.File.createTempFile;
import static java.lang.System.getProperty;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static jnr.ffi.LibraryLoader.create;
import static jnr.ffi.Runtime.getRuntime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import jnr.ffi.Pointer;
import jnr.ffi.Struct;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.NativeLongByReference;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.types.size_t;

/**
 * JNR-FFI interface to LMDB.
 *
 * <p>For performance reasons pointers are used rather than structs.
 */
final class Library {

  /**
   * Java system property name that can be set to the path of an existing directory into which the
   * LMDB system library will be extracted from the LmdbJava JAR. If unspecified the LMDB system
   * library is extracted to the <code>java.io.tmpdir</code>. Ignored if the LMDB system library is
   * not being extracted from the LmdbJava JAR (as would be the case if other system properties
   * defined in <code>TargetName</code> have been set).
   */
  public static final String LMDB_EXTRACT_DIR_PROP = "lmdbjava.extract.dir";

  /** Indicates the directory where the LMDB system library will be extracted. */
  static final String EXTRACT_DIR =
      getProperty(LMDB_EXTRACT_DIR_PROP, getProperty("java.io.tmpdir"));

  static final Lmdb LIB;
  static final jnr.ffi.Runtime RUNTIME;

  static {
    final String libToLoad;

    if (TargetName.IS_EXTERNAL) {
      libToLoad = TargetName.RESOLVED_FILENAME;
    } else {
      libToLoad = extract(TargetName.RESOLVED_FILENAME);
    }

    LIB = create(Lmdb.class).load(libToLoad);
    RUNTIME = getRuntime(LIB);
  }

  private Library() {}

  private static String extract(final String name) {
    final String suffix = name.substring(name.lastIndexOf('.'));
    final File file;
    try {
      final File dir = new File(EXTRACT_DIR);
      if (!dir.exists() || !dir.isDirectory()) {
        throw new IllegalStateException("Invalid extraction directory " + dir);
      }
      file = createTempFile("lmdbjava-native-library-", suffix, dir);
      file.deleteOnExit();
      final ClassLoader cl = currentThread().getContextClassLoader();
      try (InputStream in = cl.getResourceAsStream(name);
          OutputStream out = Files.newOutputStream(file.toPath())) {
        requireNonNull(in, "Classpath resource not found");
        int bytes;
        final byte[] buffer = new byte[4_096];
        while (-1 != (bytes = in.read(buffer))) {
          out.write(buffer, 0, bytes);
        }
      }
      return file.getAbsolutePath();
    } catch (final IOException e) {
      throw new LmdbException("Failed to extract " + name, e);
    }
  }

  /** Structure to wrap a native <code>MDB_envinfo</code>. Not for external use. */
  public static final class MDB_envinfo extends Struct {

    public final Pointer f0_me_mapaddr;
    public final size_t f1_me_mapsize;
    public final size_t f2_me_last_pgno;
    public final size_t f3_me_last_txnid;
    public final u_int32_t f4_me_maxreaders;
    public final u_int32_t f5_me_numreaders;

    MDB_envinfo(final jnr.ffi.Runtime runtime) {
      super(runtime);
      this.f0_me_mapaddr = new Pointer();
      this.f1_me_mapsize = new size_t();
      this.f2_me_last_pgno = new size_t();
      this.f3_me_last_txnid = new size_t();
      this.f4_me_maxreaders = new u_int32_t();
      this.f5_me_numreaders = new u_int32_t();
    }
  }

  /** Structure to wrap a native <code>MDB_stat</code>. Not for external use. */
  public static final class MDB_stat extends Struct {

    public final u_int32_t f0_ms_psize;
    public final u_int32_t f1_ms_depth;
    public final size_t f2_ms_branch_pages;
    public final size_t f3_ms_leaf_pages;
    public final size_t f4_ms_overflow_pages;
    public final size_t f5_ms_entries;

    MDB_stat(final jnr.ffi.Runtime runtime) {
      super(runtime);
      this.f0_ms_psize = new u_int32_t();
      this.f1_ms_depth = new u_int32_t();
      this.f2_ms_branch_pages = new size_t();
      this.f3_ms_leaf_pages = new size_t();
      this.f4_ms_overflow_pages = new size_t();
      this.f5_ms_entries = new size_t();
    }
  }

  /** Custom comparator callback used by <code>mdb_set_compare</code>. */
  public interface ComparatorCallback {

    @Delegate
    int compare(@In Pointer keyA, @In Pointer keyB);
  }

  /** JNR API for MDB-defined C functions. Not for external use. */
  public interface Lmdb {

    void mdb_cursor_close(@In Pointer cursor);

    int mdb_cursor_count(@In Pointer cursor, NativeLongByReference countp);

    int mdb_cursor_del(@In Pointer cursor, int flags);

    int mdb_cursor_get(@In Pointer cursor, Pointer k, @Out Pointer v, int cursorOp);

    int mdb_cursor_open(@In Pointer txn, @In Pointer dbi, PointerByReference cursorPtr);

    int mdb_cursor_put(@In Pointer cursor, @In Pointer key, @In Pointer data, int flags);

    int mdb_cursor_renew(@In Pointer txn, @In Pointer cursor);

    void mdb_dbi_close(@In Pointer env, @In Pointer dbi);

    int mdb_dbi_flags(@In Pointer txn, @In Pointer dbi, @Out IntByReference flags);

    int mdb_dbi_open(@In Pointer txn, @In byte[] name, int flags, @In Pointer dbiPtr);

    int mdb_del(@In Pointer txn, @In Pointer dbi, @In Pointer key, @In Pointer data);

    int mdb_drop(@In Pointer txn, @In Pointer dbi, int del);

    void mdb_env_close(@In Pointer env);

    int mdb_env_copy2(@In Pointer env, @In String path, int flags);

    int mdb_env_create(PointerByReference envPtr);

    int mdb_env_get_fd(@In Pointer env, @In Pointer fd);

    int mdb_env_get_flags(@In Pointer env, int flags);

    int mdb_env_get_maxkeysize(@In Pointer env);

    int mdb_env_get_maxreaders(@In Pointer env, int readers);

    int mdb_env_get_path(@In Pointer env, String path);

    int mdb_env_info(@In Pointer env, @Out MDB_envinfo info);

    int mdb_env_open(@In Pointer env, @In String path, int flags, int mode);

    int mdb_env_set_flags(@In Pointer env, int flags, int onoff);

    int mdb_env_set_mapsize(@In Pointer env, @size_t long size);

    int mdb_env_set_maxdbs(@In Pointer env, int dbs);

    int mdb_env_set_maxreaders(@In Pointer env, int readers);

    int mdb_env_stat(@In Pointer env, @Out MDB_stat stat);

    int mdb_env_sync(@In Pointer env, int f);

    int mdb_get(@In Pointer txn, @In Pointer dbi, @In Pointer key, @Out Pointer data);

    int mdb_put(@In Pointer txn, @In Pointer dbi, @In Pointer key, @In Pointer data, int flags);

    int mdb_reader_check(@In Pointer env, @Out IntByReference dead);

    int mdb_set_compare(@In Pointer txn, @In Pointer dbi, ComparatorCallback cb);

    int mdb_stat(@In Pointer txn, @In Pointer dbi, @Out MDB_stat stat);

    String mdb_strerror(int rc);

    void mdb_txn_abort(@In Pointer txn);

    int mdb_txn_begin(@In Pointer env, @In Pointer parentTx, int flags, Pointer txPtr);

    int mdb_txn_commit(@In Pointer txn);

    Pointer mdb_txn_env(@In Pointer txn);

    long mdb_txn_id(@In Pointer txn);

    int mdb_txn_renew(@In Pointer txn);

    void mdb_txn_reset(@In Pointer txn);

    int mdb_cmp(@In Pointer txn, @In Pointer dbi, @In Pointer key1, @In Pointer key2);

    Pointer mdb_version(IntByReference major, IntByReference minor, IntByReference patch);
  }
}
