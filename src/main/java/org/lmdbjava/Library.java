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
import static java.io.File.createTempFile;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import static java.lang.Boolean.getBoolean;
import static java.lang.System.getProperty;
import static java.lang.Thread.currentThread;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static jnr.ffi.LibraryLoader.create;
import jnr.ffi.Pointer;
import static jnr.ffi.Runtime.getRuntime;
import jnr.ffi.Struct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.NativeLongByReference;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.types.size_t;

/**
 * JNR-FFI interface to LMDB.
 *
 * <p>
 * For performance reasons pointers are used rather than structs.
 */
final class Library {

  /**
   * Java system property name that can be set to disable automatic extraction
   * of the LMDB system library from the LmdbJava JAR. This may be desirable if
   * an operating system-provided LMDB system library is preferred (eg operating
   * system package management, vendor support, special compiler flags, security
   * auditing, profile guided optimization builds, faster startup time by
   * avoiding the library copy etc).
   */
  public static final String DISABLE_EXTRACT_PROP = "lmdbjava.disable.extract";

  /**
   * Indicates whether automatic extraction of the LMDB system library is
   * permitted.
   */
  public static final boolean SHOULD_EXTRACT = !getBoolean(DISABLE_EXTRACT_PROP);

  static final Lmdb LIB;
  static final jnr.ffi.Runtime RUNTIME;
  private static final String LIB_NAME = "lmdb";

  static {
    final String libToLoad;

    final String arch = getProperty("os.arch");
    final boolean arch64 = "x64".equals(arch) || "amd64".equals(arch)
                               || "x86_64".equals(arch);

    final String os = getProperty("os.name");
    final boolean linux = os.toLowerCase(ENGLISH).startsWith("linux");
    final boolean osx = os.startsWith("Mac OS X");
    final boolean windows = os.startsWith("Windows");

    if (SHOULD_EXTRACT && arch64 && linux) {
      libToLoad = extract("org/lmdbjava/lmdbjava-native-linux-x86_64.so");
    } else if (SHOULD_EXTRACT && arch64 && osx) {
      libToLoad = extract("org/lmdbjava/lmdbjava-native-osx-x86_64.dylib");
    } else if (SHOULD_EXTRACT && arch64 && windows) {
      libToLoad = extract("org/lmdbjava/lmdbjava-native-windows-x86_64.dll");
    } else {
      libToLoad = LIB_NAME;
    }

    LIB = create(Lmdb.class).load(libToLoad);
    RUNTIME = getRuntime(LIB);
  }

  private Library() {
  }

  @SuppressWarnings("NestedAssignment")
  private static String extract(final String name) {
    final String suffix = name.substring(name.lastIndexOf('.'));
    final File file;
    try {
      file = createTempFile("lmdbjava-native-library-", suffix);
      file.deleteOnExit();
      final ClassLoader cl = currentThread().getContextClassLoader();
      try (InputStream in = cl.getResourceAsStream(name);
           OutputStream out = new FileOutputStream(file)) {
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

  /**
   * Structure to wrap a native <code>MDB_envinfo</code>. Not for external use.
   */
  @SuppressWarnings({"checkstyle:typename", "checkstyle:visibilitymodifier",
                     "checkstyle:membername"})
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

  /**
   * Structure to wrap a native <code>MDB_stat</code>. Not for external use.
   */
  @SuppressWarnings({"checkstyle:typename", "checkstyle:visibilitymodifier",
                     "checkstyle:membername"})
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

  /**
   * JNR API for MDB-defined C functions. Not for external use.
   */
  @SuppressWarnings({"checkstyle:methodname", "PMD.MethodNamingConventions"})
  public interface Lmdb {

    void mdb_cursor_close(@In Pointer cursor);

    int mdb_cursor_count(@In Pointer cursor, NativeLongByReference countp);

    int mdb_cursor_del(@In Pointer cursor, int flags);

    int mdb_cursor_get(@In Pointer cursor, Pointer k, @Out Pointer v,
                       int cursorOp);

    int mdb_cursor_open(@In Pointer txn, @In Pointer dbi,
                        PointerByReference cursorPtr);

    int mdb_cursor_put(@In Pointer cursor, @In Pointer key, @In Pointer data,
                       int flags);

    int mdb_cursor_renew(@In Pointer txn, @In Pointer cursor);

    void mdb_dbi_close(@In Pointer env, @In Pointer dbi);

    int mdb_dbi_flags(@In Pointer txn, @In Pointer dbi, int flags);

    int mdb_dbi_open(@In Pointer txn, @In byte[] name, int flags,
                     @In Pointer dbiPtr);

    int mdb_del(@In Pointer txn, @In Pointer dbi, @In Pointer key,
                @In Pointer data);

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

    int mdb_get(@In Pointer txn, @In Pointer dbi, @In Pointer key,
                @Out Pointer data);

    int mdb_put(@In Pointer txn, @In Pointer dbi, @In Pointer key,
                @In Pointer data,
                int flags);

    int mdb_reader_check(@In Pointer env, int dead);

    int mdb_stat(@In Pointer txn, @In Pointer dbi, @Out MDB_stat stat);

    String mdb_strerror(int rc);

    void mdb_txn_abort(@In Pointer txn);

    int mdb_txn_begin(@In Pointer env, @In Pointer parentTx, int flags,
                      Pointer txPtr);

    int mdb_txn_commit(@In Pointer txn);

    Pointer mdb_txn_env(@In Pointer txn);

    long mdb_txn_id(@In Pointer txn);

    int mdb_txn_renew(@In Pointer txn);

    void mdb_txn_reset(@In Pointer txn);

    Pointer mdb_version(IntByReference major, IntByReference minor,
                        IntByReference patch);

  }
}
