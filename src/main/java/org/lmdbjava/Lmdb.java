package org.lmdbjava;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.System.getProperty;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;

/**
 * Complete LMDB wrapper using Java Foreign Function & Memory API.
 * Requires Java 22+ with --enable-preview or Java 23+.
 */
public class Lmdb {

  /**
   * Java system property name that can be set to the path of an existing directory into which the
   * LMDB system library will be extracted from the LmdbJava JAR. If unspecified the LMDB system
   * library is extracted to the <code>tmpdir</code>. Ignored if the LMDB system library is
   * not being extracted from the LmdbJava JAR (as would be the case if other system properties
   * defined in <code>TargetName</code> have been set).
   */
  public static final String LMDB_EXTRACT_DIR_PROP = "lmdbjava.extract.dir";

  /**
   * Indicates the directory where the LMDB system library will be extracted.
   */
  static final String EXTRACT_DIR =
          getProperty(LMDB_EXTRACT_DIR_PROP, getProperty("java.io.tmpdir"));

  // ============================================================================
  // LIBRARY INITIALIZATION
  // ============================================================================

  private static final SymbolLookup LIBRARY;
  private static final Linker LINKER = Linker.nativeLinker();

  static {
    try {
      final Path libToLoad;

      if (TargetName.IS_EXTERNAL) {
        libToLoad = Paths.get(TargetName.RESOLVED_FILENAME);
      } else {
        libToLoad = extract(TargetName.RESOLVED_FILENAME);
      }
      LIBRARY = SymbolLookup.libraryLookup(libToLoad, Arena.global());
    } catch (Exception e) {
      throw new ExceptionInInitializerError("Failed to load LMDB library: " + e.getMessage());
    }
  }

  // ============================================================================
  // CONSTANTS AND FLAGS
  // ============================================================================

  public static final class MDB {
    // Environment flags
    public static final int FIXEDMAP = 0x01;
    public static final int NOSUBDIR = 0x4000;
    public static final int NOSYNC = 0x10000;
    public static final int RDONLY = 0x20000;
    public static final int NOMETASYNC = 0x40000;
    public static final int WRITEMAP = 0x80000;
    public static final int MAPASYNC = 0x100000;
    public static final int NOTLS = 0x200000;
    public static final int NOLOCK = 0x400000;
    public static final int NORDAHEAD = 0x800000;
    public static final int NOMEMINIT = 0x1000000;
    public static final int PREVSNAPSHOT = 0x2000000;

    // Database flags
    public static final int REVERSEKEY = 0x02;
    public static final int DUPSORT = 0x04;
    public static final int INTEGERKEY = 0x08;
    public static final int DUPFIXED = 0x10;
    public static final int INTEGERDUP = 0x20;
    public static final int REVERSEDUP = 0x40;
    public static final int CREATE = 0x40000;

    // Write flags
    public static final int NOOVERWRITE = 0x10;
    public static final int NODUPDATA = 0x20;
    public static final int CURRENT = 0x40;
    public static final int RESERVE = 0x10000;
    public static final int APPEND = 0x20000;
    public static final int APPENDDUP = 0x40000;
    public static final int MULTIPLE = 0x80000;

    // Copy flags
    public static final int CP_COMPACT = 0x01;

    // Cursor operations
    public static final int FIRST = 0;
    public static final int FIRST_DUP = 1;
    public static final int GET_BOTH = 2;
    public static final int GET_BOTH_RANGE = 3;
    public static final int GET_CURRENT = 4;
    public static final int GET_MULTIPLE = 5;
    public static final int LAST = 6;
    public static final int LAST_DUP = 7;
    public static final int NEXT = 8;
    public static final int NEXT_DUP = 9;
    public static final int NEXT_MULTIPLE = 10;
    public static final int NEXT_NODUP = 11;
    public static final int PREV = 12;
    public static final int PREV_DUP = 13;
    public static final int PREV_NODUP = 14;
    public static final int SET = 15;
    public static final int SET_KEY = 16;
    public static final int SET_RANGE = 17;
    public static final int PREV_MULTIPLE = 18;

    // Return codes
    public static final int SUCCESS = 0;
    public static final int KEYEXIST = -30799;
    public static final int NOTFOUND = -30798;
    public static final int PAGE_NOTFOUND = -30797;
    public static final int CORRUPTED = -30796;
    public static final int PANIC = -30795;
    public static final int VERSION_MISMATCH = -30794;
    public static final int INVALID = -30793;
    public static final int MAP_FULL = -30792;
    public static final int DBS_FULL = -30791;
    public static final int READERS_FULL = -30790;
    public static final int TLS_FULL = -30789;
    public static final int TXN_FULL = -30788;
    public static final int CURSOR_FULL = -30787;
    public static final int PAGE_FULL = -30786;
    public static final int MAP_RESIZED = -30785;
    public static final int INCOMPATIBLE = -30784;
    public static final int BAD_RSLOT = -30783;
    public static final int BAD_TXN = -30782;
    public static final int BAD_VALSIZE = -30781;
    public static final int BAD_DBI = -30780;
  }

  // ============================================================================
  // STRUCTURES
  // ============================================================================

  /**
   * MDB_val structure
   */
  public static final class MDB_val {
    private static final StructLayout LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("mv_size"),
            ValueLayout.ADDRESS.withName("mv_data")
    );

    private static final VarHandle MV_SIZE = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("mv_size")
    );

    private static final VarHandle MV_DATA = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("mv_data")
    );

    private final MemorySegment segment;

    public MDB_val(Arena arena) {
      this.segment = arena.allocate(LAYOUT);
    }

    public MDB_val(MemorySegment segment) {
      this.segment = segment;
    }

    public long mvSize() {
      return (long) MV_SIZE.get(segment, 0L);
    }

    public void mvSize(long value) {
      MV_SIZE.set(segment, 0L, value);
    }

    public MemorySegment mvData() {
      return (MemorySegment) MV_DATA.get(segment, 0L);
    }

    public void mvData(MemorySegment value) {
      MV_DATA.set(segment, 0L, value);
    }

    public MemorySegment segment() {
      return segment;
    }

    public static StructLayout layout() {
      return LAYOUT;
    }
  }

  /**
   * MDB_arr_val structure
   */
  public static final class MDB_arr_val {
    private static final StructLayout LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("mv_size1"),
            ValueLayout.ADDRESS.withName("mv_data1"),
            ValueLayout.JAVA_LONG.withName("mv_size2"),
            ValueLayout.ADDRESS.withName("mv_data2")
    );

    private static final VarHandle MV_SIZE1 = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("mv_size1")
    );

    private static final VarHandle MV_DATA1 = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("mv_data1")
    );

    private static final VarHandle MV_SIZE2 = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("mv_size2")
    );

    private static final VarHandle MV_DATA2 = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("mv_data2")
    );

    private final MemorySegment segment;

    public MDB_arr_val(Arena arena) {
      this.segment = arena.allocate(LAYOUT);
    }

    public MDB_arr_val(MemorySegment segment) {
      this.segment = segment;
    }

    public long mvSize1() {
      return (long) MV_SIZE1.get(segment, 0L);
    }

    public void mvSize1(long value) {
      MV_SIZE1.set(segment, 0L, value);
    }

    public MemorySegment mvData1() {
      return (MemorySegment) MV_DATA1.get(segment, 0L);
    }

    public void mvData1(MemorySegment value) {
      MV_DATA1.set(segment, 0L, value);
    }

    public long mvSize2() {
      return (long) MV_SIZE2.get(segment, 0L);
    }

    public void mvSize2(long value) {
      MV_SIZE2.set(segment, 0L, value);
    }

    public MemorySegment mvData2() {
      return (MemorySegment) MV_DATA2.get(segment, 0L);
    }

    public void mvData2(MemorySegment value) {
      MV_DATA2.set(segment, 0L, value);
    }

    public MemorySegment segment() {
      return segment;
    }

    public static StructLayout layout() {
      return LAYOUT;
    }
  }

  /**
   * MDB_stat structure
   */
  public static final class MDB_stat {
    private static final StructLayout LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_INT.withName("ms_psize"),
            ValueLayout.JAVA_INT.withName("ms_depth"),
            ValueLayout.JAVA_LONG.withName("ms_branch_pages"),
            ValueLayout.JAVA_LONG.withName("ms_leaf_pages"),
            ValueLayout.JAVA_LONG.withName("ms_overflow_pages"),
            ValueLayout.JAVA_LONG.withName("ms_entries")
    );

    private static final VarHandle MS_PSIZE = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("ms_psize")
    );

    private static final VarHandle MS_DEPTH = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("ms_depth")
    );

    private static final VarHandle MS_BRANCH_PAGES = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("ms_branch_pages")
    );

    private static final VarHandle MS_LEAF_PAGES = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("ms_leaf_pages")
    );

    private static final VarHandle MS_OVERFLOW_PAGES = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("ms_overflow_pages")
    );

    private static final VarHandle MS_ENTRIES = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("ms_entries")
    );

    private final MemorySegment segment;

    public MDB_stat(Arena arena) {
      this.segment = arena.allocate(LAYOUT);
    }

    public MDB_stat(MemorySegment segment) {
      this.segment = segment;
    }

    public int msPsize() {
      return (int) MS_PSIZE.get(segment, 0L);
    }

    public int msDepth() {
      return (int) MS_DEPTH.get(segment, 0L);
    }

    public long msBranchPages() {
      return (long) MS_BRANCH_PAGES.get(segment, 0L);
    }

    public long msLeafPages() {
      return (long) MS_LEAF_PAGES.get(segment, 0L);
    }

    public long msOverflowPages() {
      return (long) MS_OVERFLOW_PAGES.get(segment, 0L);
    }

    public long msEntries() {
      return (long) MS_ENTRIES.get(segment, 0L);
    }

    public MemorySegment segment() {
      return segment;
    }

    public static StructLayout layout() {
      return LAYOUT;
    }
  }

  /**
   * MDB_envinfo structure
   */
  public static final class MDB_envinfo {
    static final StructLayout LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("me_mapaddr"),
            ValueLayout.JAVA_LONG.withName("me_mapsize"),
            ValueLayout.JAVA_LONG.withName("me_last_pgno"),
            ValueLayout.JAVA_LONG.withName("me_last_txnid"),
            ValueLayout.JAVA_INT.withName("me_maxreaders"),
            ValueLayout.JAVA_INT.withName("me_numreaders")
    );

    private static final VarHandle ME_MAPADDR = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("me_mapaddr")
    );

    private static final VarHandle ME_MAPSIZE = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("me_mapsize")
    );

    private static final VarHandle ME_LAST_PGNO = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("me_last_pgno")
    );

    private static final VarHandle ME_LAST_TXNID = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("me_last_txnid")
    );

    private static final VarHandle ME_MAXREADERS = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("me_maxreaders")
    );

    private static final VarHandle ME_NUMREADERS = LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("me_numreaders")
    );

    private final MemorySegment segment;

    public MDB_envinfo(Arena arena) {
      this.segment = arena.allocate(LAYOUT);
    }

    public MDB_envinfo(MemorySegment segment) {
      this.segment = segment;
    }

    public MemorySegment meMapaddr() {
      return (MemorySegment) ME_MAPADDR.get(segment, 0L);
    }

    public long meMapsize() {
      return (long) ME_MAPSIZE.get(segment, 0L);
    }

    public long meLastPgno() {
      return (long) ME_LAST_PGNO.get(segment, 0L);
    }

    public long meLastTxnid() {
      return (long) ME_LAST_TXNID.get(segment, 0L);
    }

    public int meMaxreaders() {
      return (int) ME_MAXREADERS.get(segment, 0L);
    }

    public int meNumreaders() {
      return (int) ME_NUMREADERS.get(segment, 0L);
    }

    public MemorySegment segment() {
      return segment;
    }

    public static StructLayout layout() {
      return LAYOUT;
    }
  }

  // ============================================================================
  // FUNCTION DESCRIPTORS
  // ============================================================================

  // Version information
  private static final FunctionDescriptor MDB_VERSION_DESC = FunctionDescriptor.of(
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // Environment functions
  private static final FunctionDescriptor MDB_ENV_CREATE_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_OPEN_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_COPY_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_COPYFD_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_COPY2_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_COPYFD2_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_STAT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_INFO_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_SYNC_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_CLOSE_DESC = FunctionDescriptor.ofVoid(
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_SET_FLAGS_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_GET_FLAGS_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_GET_PATH_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_GET_FD_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_SET_MAPSIZE_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG
  );

  private static final FunctionDescriptor MDB_ENV_SET_MAXREADERS_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_GET_MAXREADERS_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_SET_MAXDBS_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_ENV_GET_MAXKEYSIZE_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_SET_USERCTX_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_ENV_GET_USERCTX_DESC = FunctionDescriptor.of(
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // Transaction functions
  private static final FunctionDescriptor MDB_TXN_BEGIN_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_TXN_ENV_DESC = FunctionDescriptor.of(
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_TXN_ID_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_LONG,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_TXN_COMMIT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_TXN_ABORT_DESC = FunctionDescriptor.ofVoid(
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_TXN_RESET_DESC = FunctionDescriptor.ofVoid(
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_TXN_RENEW_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  // Database functions
  private static final FunctionDescriptor MDB_DBI_OPEN_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_STAT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_DBI_FLAGS_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_DBI_CLOSE_DESC = FunctionDescriptor.ofVoid(
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_DROP_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.JAVA_INT
  );

  // Data access functions
  private static final FunctionDescriptor MDB_GET_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_PUT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_DEL_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // Cursor functions
  private static final FunctionDescriptor MDB_CURSOR_OPEN_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_CURSOR_CLOSE_DESC = FunctionDescriptor.ofVoid(
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_CURSOR_RENEW_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_CURSOR_TXN_DESC = FunctionDescriptor.of(
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_CURSOR_DBI_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_CURSOR_GET_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_CURSOR_PUT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_CURSOR_DEL_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_CURSOR_COUNT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // Comparison functions
  private static final FunctionDescriptor MDB_CMP_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_DCMP_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // Set comparison functions
  private static final FunctionDescriptor MDB_SET_COMPARE_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_SET_DUPSORT_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS
  );

  // Comparison function callback descriptor (MDB_cmp_func)
  private static final FunctionDescriptor MDB_CMP_FUNC_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // Utility functions
  private static final FunctionDescriptor MDB_STRERROR_DESC = FunctionDescriptor.of(
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_INT
  );

  private static final FunctionDescriptor MDB_READER_LIST_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  private static final FunctionDescriptor MDB_READER_CHECK_DESC = FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS
  );

  // ============================================================================
  // METHOD HANDLES
  // ============================================================================

  private static final MethodHandle mdb_version;

  private static final MethodHandle mdb_env_create;
  private static final MethodHandle mdb_env_open;
  private static final MethodHandle mdb_env_copy;
  private static final MethodHandle mdb_env_copyfd;
  private static final MethodHandle mdb_env_copy2;
  private static final MethodHandle mdb_env_copyfd2;
  private static final MethodHandle mdb_env_stat;
  private static final MethodHandle mdb_env_info;
  private static final MethodHandle mdb_env_sync;
  private static final MethodHandle mdb_env_close;
  private static final MethodHandle mdb_env_set_flags;
  private static final MethodHandle mdb_env_get_flags;
  private static final MethodHandle mdb_env_get_path;
  private static final MethodHandle mdb_env_get_fd;
  private static final MethodHandle mdb_env_set_mapsize;
  private static final MethodHandle mdb_env_set_maxreaders;
  private static final MethodHandle mdb_env_get_maxreaders;
  private static final MethodHandle mdb_env_set_maxdbs;
  private static final MethodHandle mdb_env_get_maxkeysize;
  private static final MethodHandle mdb_env_set_userctx;
  private static final MethodHandle mdb_env_get_userctx;

  private static final MethodHandle mdb_txn_begin;
  private static final MethodHandle mdb_txn_env;
  private static final MethodHandle mdb_txn_id;
  private static final MethodHandle mdb_txn_commit;
  private static final MethodHandle mdb_txn_abort;
  private static final MethodHandle mdb_txn_reset;
  private static final MethodHandle mdb_txn_renew;

  private static final MethodHandle mdb_dbi_open;
  private static final MethodHandle mdb_stat;
  private static final MethodHandle mdb_dbi_flags;
  private static final MethodHandle mdb_dbi_close;
  private static final MethodHandle mdb_drop;

  private static final MethodHandle mdb_get;
  private static final MethodHandle mdb_put;
  private static final MethodHandle mdb_del;

  private static final MethodHandle mdb_cursor_open;
  private static final MethodHandle mdb_cursor_close;
  private static final MethodHandle mdb_cursor_renew;
  private static final MethodHandle mdb_cursor_txn;
  private static final MethodHandle mdb_cursor_dbi;
  private static final MethodHandle mdb_cursor_get;
  private static final MethodHandle mdb_cursor_put;
  private static final MethodHandle mdb_cursor_del;
  private static final MethodHandle mdb_cursor_count;

  private static final MethodHandle mdb_cmp;
  private static final MethodHandle mdb_dcmp;
  private static final MethodHandle mdb_set_compare;
  private static final MethodHandle mdb_set_dupsort;

  private static final MethodHandle mdb_strerror;
  private static final MethodHandle mdb_reader_list;
  private static final MethodHandle mdb_reader_check;

  static {
    try {
      mdb_version = LINKER.downcallHandle(
              LIBRARY.find("mdb_version").orElseThrow(),
              MDB_VERSION_DESC
      );

      mdb_env_create = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_create").orElseThrow(),
              MDB_ENV_CREATE_DESC
      );

      mdb_env_open = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_open").orElseThrow(),
              MDB_ENV_OPEN_DESC
      );

      mdb_env_copy = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_copy").orElseThrow(),
              MDB_ENV_COPY_DESC
      );

      mdb_env_copyfd = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_copyfd").orElseThrow(),
              MDB_ENV_COPYFD_DESC
      );

      mdb_env_copy2 = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_copy2").orElseThrow(),
              MDB_ENV_COPY2_DESC
      );

      mdb_env_copyfd2 = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_copyfd2").orElseThrow(),
              MDB_ENV_COPYFD2_DESC
      );

      mdb_env_stat = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_stat").orElseThrow(),
              MDB_ENV_STAT_DESC
      );

      mdb_env_info = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_info").orElseThrow(),
              MDB_ENV_INFO_DESC
      );

      mdb_env_sync = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_sync").orElseThrow(),
              MDB_ENV_SYNC_DESC
      );

      mdb_env_close = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_close").orElseThrow(),
              MDB_ENV_CLOSE_DESC
      );

      mdb_env_set_flags = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_set_flags").orElseThrow(),
              MDB_ENV_SET_FLAGS_DESC
      );

      mdb_env_get_flags = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_get_flags").orElseThrow(),
              MDB_ENV_GET_FLAGS_DESC
      );

      mdb_env_get_path = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_get_path").orElseThrow(),
              MDB_ENV_GET_PATH_DESC
      );

      mdb_env_get_fd = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_get_fd").orElseThrow(),
              MDB_ENV_GET_FD_DESC
      );

      mdb_env_set_mapsize = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_set_mapsize").orElseThrow(),
              MDB_ENV_SET_MAPSIZE_DESC
      );

      mdb_env_set_maxreaders = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_set_maxreaders").orElseThrow(),
              MDB_ENV_SET_MAXREADERS_DESC
      );

      mdb_env_get_maxreaders = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_get_maxreaders").orElseThrow(),
              MDB_ENV_GET_MAXREADERS_DESC
      );

      mdb_env_set_maxdbs = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_set_maxdbs").orElseThrow(),
              MDB_ENV_SET_MAXDBS_DESC
      );

      mdb_env_get_maxkeysize = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_get_maxkeysize").orElseThrow(),
              MDB_ENV_GET_MAXKEYSIZE_DESC
      );

      mdb_env_set_userctx = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_set_userctx").orElseThrow(),
              MDB_ENV_SET_USERCTX_DESC
      );

      mdb_env_get_userctx = LINKER.downcallHandle(
              LIBRARY.find("mdb_env_get_userctx").orElseThrow(),
              MDB_ENV_GET_USERCTX_DESC
      );

      mdb_txn_begin = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_begin").orElseThrow(),
              MDB_TXN_BEGIN_DESC
      );

      mdb_txn_env = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_env").orElseThrow(),
              MDB_TXN_ENV_DESC
      );

      mdb_txn_id = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_id").orElseThrow(),
              MDB_TXN_ID_DESC
      );

      mdb_txn_commit = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_commit").orElseThrow(),
              MDB_TXN_COMMIT_DESC
      );

      mdb_txn_abort = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_abort").orElseThrow(),
              MDB_TXN_ABORT_DESC
      );

      mdb_txn_reset = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_reset").orElseThrow(),
              MDB_TXN_RESET_DESC
      );

      mdb_txn_renew = LINKER.downcallHandle(
              LIBRARY.find("mdb_txn_renew").orElseThrow(),
              MDB_TXN_RENEW_DESC
      );

      mdb_dbi_open = LINKER.downcallHandle(
              LIBRARY.find("mdb_dbi_open").orElseThrow(),
              MDB_DBI_OPEN_DESC
      );

      mdb_stat = LINKER.downcallHandle(
              LIBRARY.find("mdb_stat").orElseThrow(),
              MDB_STAT_DESC
      );

      mdb_dbi_flags = LINKER.downcallHandle(
              LIBRARY.find("mdb_dbi_flags").orElseThrow(),
              MDB_DBI_FLAGS_DESC
      );

      mdb_dbi_close = LINKER.downcallHandle(
              LIBRARY.find("mdb_dbi_close").orElseThrow(),
              MDB_DBI_CLOSE_DESC
      );

      mdb_drop = LINKER.downcallHandle(
              LIBRARY.find("mdb_drop").orElseThrow(),
              MDB_DROP_DESC
      );

      mdb_get = LINKER.downcallHandle(
              LIBRARY.find("mdb_get").orElseThrow(),
              MDB_GET_DESC
      );

      mdb_put = LINKER.downcallHandle(
              LIBRARY.find("mdb_put").orElseThrow(),
              MDB_PUT_DESC
      );

      mdb_del = LINKER.downcallHandle(
              LIBRARY.find("mdb_del").orElseThrow(),
              MDB_DEL_DESC
      );

      mdb_cursor_open = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_open").orElseThrow(),
              MDB_CURSOR_OPEN_DESC
      );

      mdb_cursor_close = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_close").orElseThrow(),
              MDB_CURSOR_CLOSE_DESC
      );

      mdb_cursor_renew = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_renew").orElseThrow(),
              MDB_CURSOR_RENEW_DESC
      );

      mdb_cursor_txn = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_txn").orElseThrow(),
              MDB_CURSOR_TXN_DESC
      );

      mdb_cursor_dbi = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_dbi").orElseThrow(),
              MDB_CURSOR_DBI_DESC
      );

      mdb_cursor_get = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_get").orElseThrow(),
              MDB_CURSOR_GET_DESC
      );

      mdb_cursor_put = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_put").orElseThrow(),
              MDB_CURSOR_PUT_DESC
      );

      mdb_cursor_del = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_del").orElseThrow(),
              MDB_CURSOR_DEL_DESC
      );

      mdb_cursor_count = LINKER.downcallHandle(
              LIBRARY.find("mdb_cursor_count").orElseThrow(),
              MDB_CURSOR_COUNT_DESC
      );

      mdb_cmp = LINKER.downcallHandle(
              LIBRARY.find("mdb_cmp").orElseThrow(),
              MDB_CMP_DESC
      );

      mdb_dcmp = LINKER.downcallHandle(
              LIBRARY.find("mdb_dcmp").orElseThrow(),
              MDB_DCMP_DESC
      );

      mdb_set_compare = LINKER.downcallHandle(
              LIBRARY.find("mdb_set_compare").orElseThrow(),
              MDB_SET_COMPARE_DESC
      );

      mdb_set_dupsort = LINKER.downcallHandle(
              LIBRARY.find("mdb_set_dupsort").orElseThrow(),
              MDB_SET_DUPSORT_DESC
      );

      mdb_strerror = LINKER.downcallHandle(
              LIBRARY.find("mdb_strerror").orElseThrow(),
              MDB_STRERROR_DESC
      );

      mdb_reader_list = LINKER.downcallHandle(
              LIBRARY.find("mdb_reader_list").orElseThrow(),
              MDB_READER_LIST_DESC
      );

      mdb_reader_check = LINKER.downcallHandle(
              LIBRARY.find("mdb_reader_check").orElseThrow(),
              MDB_READER_CHECK_DESC
      );

    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Comparator interface for custom key comparison
   */
  @FunctionalInterface
  public interface ComparatorCallback {
    /**
     * Compare two MDB_val structures
     *
     * @return < 0 if a < b, 0 if a == b, > 0 if a > b
     */
    int compare(MDB_val a, MDB_val b);
  }

  // ============================================================================
  // PUBLIC API METHODS
  // ============================================================================

  // Version
  public static String mdb_version(MemorySegment major, MemorySegment minor, MemorySegment patch) {
    try {
      MemorySegment result = (MemorySegment) mdb_version.invoke(major, minor, patch);
      return result.reinterpret(Long.MAX_VALUE).getString(0);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_version", t.getMessage(), t);
    }
  }

  // Environment functions
  public static int mdb_env_create(MemorySegment envPtr) {
    try {
      return (int) mdb_env_create.invoke(envPtr);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_create", t.getMessage(), t);
    }
  }

  public static int mdb_env_open(MemorySegment env, MemorySegment pathSegment, int flags, int mode) {
    try {
      return (int) mdb_env_open.invoke(env, pathSegment, flags, mode);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_open", t.getMessage(), t);
    }
  }

  public static int mdb_env_copy(MemorySegment env, MemorySegment pathSegment) {
    try {
      return (int) mdb_env_copy.invoke(env, pathSegment);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_copy", t.getMessage(), t);
    }
  }

  public static int mdb_env_copyfd(MemorySegment env, int fd) {
    try {
      return (int) mdb_env_copyfd.invoke(env, fd);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_copyfd", t.getMessage(), t);
    }
  }

  public static int mdb_env_copy2(MemorySegment env, MemorySegment pathSegment, int flags) {
    try {
      return (int) mdb_env_copy2.invoke(env, pathSegment, flags);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_copy2", t.getMessage(), t);
    }
  }

  public static int mdb_env_copyfd2(MemorySegment env, int fd, int flags) {
    try {
      return (int) mdb_env_copyfd2.invoke(env, fd, flags);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_copyfd2", t.getMessage(), t);
    }
  }

  public static int mdb_env_stat(MemorySegment env, MDB_stat stat) {
    try {
      return (int) mdb_env_stat.invoke(env, stat.segment());
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_stat", t.getMessage(), t);
    }
  }

  public static int mdb_env_info(MemorySegment env, MDB_envinfo info) {
    try {
      return (int) mdb_env_info.invoke(env, info.segment());
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_info", t.getMessage(), t);
    }
  }

  public static int mdb_env_sync(MemorySegment env, int f) {
    try {
      return (int) mdb_env_sync.invoke(env, f);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_sync", t.getMessage(), t);
    }
  }

  public static void mdb_env_close(MemorySegment env) {
    try {
      mdb_env_close.invoke(env);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_close", t.getMessage(), t);
    }
  }

  public static int mdb_env_set_flags(MemorySegment env, int flags, boolean onoff) {
    try {
      return (int) mdb_env_set_flags.invoke(env, flags, onoff ? 1 : 0);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_set_flags", t.getMessage(), t);
    }
  }

//  public static int mdb_env_get_flags(MemorySegment env, Arena arena) {
//    try {
//      MemorySegment flagsPtr = arena.allocate(ValueLayout.JAVA_INT);
//      int rc = (int) mdb_env_get_flags.invoke(env, flagsPtr);
//      checkError(rc, "mdb_env_get_flags");
//      return flagsPtr.get(ValueLayout.JAVA_INT, 0);
//    } catch (final Throwable t) {
//      throw new LmdbInvokeException("mdb_env_get_flags", t.getMessage(), t);
//    }
//  }

//  public static String mdb_env_get_path(MemorySegment env, Arena arena) {
//    try {
//      MemorySegment pathPtr = arena.allocate(ValueLayout.ADDRESS);
//      int rc = (int) mdb_env_get_path.invoke(env, pathPtr);
//      checkError(rc, "mdb_env_get_path");
//      MemorySegment path = pathPtr.get(ValueLayout.ADDRESS, 0);
//      return path.reinterpret(Long.MAX_VALUE).getString(0);
//    } catch (final Throwable t) {
//      throw new LmdbInvokeException("mdb_env_get_path", t.getMessage(), t);
//    }
//  }

//  public static int mdb_env_get_fd(MemorySegment env, Arena arena) {
//    try {
//      MemorySegment fdPtr = arena.allocate(ValueLayout.JAVA_INT);
//      int rc = (int) mdb_env_get_fd.invoke(env, fdPtr);
//      checkError(rc, "mdb_env_get_fd");
//      return fdPtr.get(ValueLayout.JAVA_INT, 0);
//    } catch (final Throwable t) {
//      throw new LmdbInvokeException("mdb_env_get_fd", t.getMessage(), t);
//    }
//  }

  public static int mdb_env_set_mapsize(MemorySegment env, long size) {
    try {
      return (int) mdb_env_set_mapsize.invoke(env, size);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_set_mapsize", t.getMessage(), t);
    }
  }

  public static int mdb_env_set_maxreaders(MemorySegment env, int readers) {
    try {
      return (int) mdb_env_set_maxreaders.invoke(env, readers);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_set_maxreaders", t.getMessage(), t);
    }
  }

  public static int mdb_env_get_maxreaders(MemorySegment env, Arena arena) {
    try {
      MemorySegment readersPtr = arena.allocate(ValueLayout.JAVA_INT);
      return (int) mdb_env_get_maxreaders.invoke(env, readersPtr);
//      checkError(rc, "mdb_env_get_maxreaders");
//      return readersPtr.get(ValueLayout.JAVA_INT, 0);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_get_maxreaders", t.getMessage(), t);
    }
  }

  public static int mdb_env_set_maxdbs(MemorySegment env, int dbs) {
    try {
      return (int) mdb_env_set_maxdbs.invoke(env, dbs);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_set_maxdbs", t.getMessage(), t);
    }
  }

  public static int mdb_env_get_maxkeysize(MemorySegment env) {
    try {
      return (int) mdb_env_get_maxkeysize.invoke(env);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_get_maxkeysize", t.getMessage(), t);
    }
  }

  public static int mdb_env_set_userctx(MemorySegment env, MemorySegment ctx) {
    try {
      return  (int) mdb_env_set_userctx.invoke(env, ctx);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_set_userctx", t.getMessage(), t);
    }
  }

  public static MemorySegment mdb_env_get_userctx(MemorySegment env) {
    try {
      return (MemorySegment) mdb_env_get_userctx.invoke(env);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_env_get_userctx", t.getMessage(), t);
    }
  }

  // Transaction functions
  public static int mdb_txn_begin(MemorySegment env, MemorySegment parent, int flags, MemorySegment txnPtr) {
    try {
      return (int) mdb_txn_begin.invoke(env, parent, flags, txnPtr);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_begin", t.getMessage(), t);
    }
  }

  public static MemorySegment mdb_txn_env(MemorySegment txn) {
    try {
      return (MemorySegment) mdb_txn_env.invoke(txn);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_env", t.getMessage(), t);
    }
  }

  public static long mdb_txn_id(MemorySegment txn) {
    try {
      return (long) mdb_txn_id.invoke(txn);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_id", t.getMessage(), t);
    }
  }

  public static int mdb_txn_commit(MemorySegment txn) {
    try {
      return (int) mdb_txn_commit.invoke(txn);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_commit", t.getMessage(), t);
    }
  }

  public static void mdb_txn_abort(MemorySegment txn) {
    try {
      mdb_txn_abort.invoke(txn);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_abort", t.getMessage(), t);
    }
  }

  public static void mdb_txn_reset(MemorySegment txn) {
    try {
      mdb_txn_reset.invoke(txn);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_reset", t.getMessage(), t);
    }
  }

  public static int mdb_txn_renew(MemorySegment txn) {
    try {
      return (int) mdb_txn_renew.invoke(txn);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_txn_renew", t.getMessage(), t);
    }
  }

  // Database functions
  public static int mdb_dbi_open(MemorySegment txn, MemorySegment name, int flags, MemorySegment dbiPtr) {
    try {
      return (int) mdb_dbi_open.invoke(txn, name, flags, dbiPtr);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_dbi_open", t.getMessage(), t);
    }
  }

  public static int mdb_stat(MemorySegment txn, int dbi, MDB_stat stat) {
    try {
      return (int) mdb_stat.invoke(txn, dbi, stat.segment());
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_stat", t.getMessage(), t);
    }
  }

  public static int mdb_dbi_flags(MemorySegment txn, int dbi, MemorySegment flagsPtr) {
    try {
      return (int) mdb_dbi_flags.invoke(txn, dbi, flagsPtr);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_dbi_flags", t.getMessage(), t);
    }
  }

  public static void mdb_dbi_close(MemorySegment env, int dbi) {
    try {
      mdb_dbi_close.invoke(env, dbi);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_dbi_close", t.getMessage(), t);
    }
  }

  public static int mdb_drop(MemorySegment txn, int dbi, int del) {
    try {
      return (int) mdb_drop.invoke(txn, dbi, del);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_drop", t.getMessage(), t);
    }
  }

  // Data access functions
  public static int mdb_get(MemorySegment txn, int dbi, MDB_val key, MDB_val val) {
    try {
      return (int) mdb_get.invoke(txn, dbi, key.segment(), val.segment());
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_get", t.getMessage(), t);
    }
  }

  public static int mdb_put(MemorySegment txn, int dbi, MDB_val key, MDB_val value, int flags) {
    try {
      return (int) mdb_put.invoke(txn, dbi, key.segment(), value.segment(), flags);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_put", t.getMessage(), t);
    }
  }

  public static int mdb_del(MemorySegment txn, int dbi, MDB_val key, MDB_val value) {
    try {
      return (int) mdb_del.invoke(txn, dbi, key.segment(),
              value != null ? value.segment() : MemorySegment.NULL);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_del", t.getMessage(), t);
    }
  }

  // Cursor functions
  public static int mdb_cursor_open(MemorySegment txn, int dbi, MemorySegment cursorPtr) {
    try {
      return (int) mdb_cursor_open.invoke(txn, dbi, cursorPtr);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_open", t.getMessage(), t);
    }
  }

  public static void mdb_cursor_close(MemorySegment cursor) {
    try {
      mdb_cursor_close.invoke(cursor);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_close", t.getMessage(), t);
    }
  }

  public static int mdb_cursor_renew(MemorySegment txn, MemorySegment cursor) {
    try {
      return (int) mdb_cursor_renew.invoke(txn, cursor);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_renew", t.getMessage(), t);
    }
  }

  public static MemorySegment mdb_cursor_txn(MemorySegment cursor) {
    try {
      return (MemorySegment) mdb_cursor_txn.invoke(cursor);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_txn", t.getMessage(), t);
    }
  }

  public static int mdb_cursor_dbi(MemorySegment cursor) {
    try {
      return (int) mdb_cursor_dbi.invoke(cursor);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_dbi", t.getMessage(), t);
    }
  }

  public static int mdb_cursor_get(MemorySegment cursor, MDB_val key, MDB_val data, int op) {
    try {
      return (int) mdb_cursor_get.invoke(cursor, key.segment(), data.segment(), op);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_get", t.getMessage(), t);
    }
  }

  public static int mdb_cursor_put(MemorySegment cursor, MDB_val key, MDB_val data, int flags) {
    try {
      return (int) mdb_cursor_put.invoke(cursor, key.segment(), data.segment(), flags);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_put", t.getMessage(), t);
    }
  }

  public static int mdb_cursor_del(MemorySegment cursor, int flags) {
    try {
      return (int) mdb_cursor_del.invoke(cursor, flags);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_del", t.getMessage(), t);
    }
  }

  public static int mdb_cursor_count(MemorySegment cursor, MemorySegment countPtr) {
    try {
      return (int) mdb_cursor_count.invoke(cursor, countPtr);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cursor_count", t.getMessage(), t);
    }
  }

  // Comparison functions
  public static int mdb_cmp(MemorySegment txn, int dbi, MDB_val a, MDB_val b) {
    try {
      return (int) mdb_cmp.invoke(txn, dbi, a.segment(), b.segment());
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cmp", t.getMessage(), t);
    }
  }

  public static int mdb_dcmp(MemorySegment txn, int dbi, MDB_val a, MDB_val b) {
    try {
      return (int) mdb_dcmp.invoke(txn, dbi, a.segment(), b.segment());
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_dcmp", t.getMessage(), t);
    }
  }

  /**
   * Set a custom key comparison function for a database.
   * Must be called before any data access operations.
   * The Arena must remain alive for the lifetime of the database.
   */
  public static int mdb_set_compare(MemorySegment txn, int dbi, ComparatorCallback comparator, Arena arena) {
    try {
      // Create a method handle that adapts the comparator
      MethodHandle adapterHandle = createComparatorAdapter(comparator);

      // Create upcall stub
      MemorySegment stub = LINKER.upcallStub(adapterHandle, MDB_CMP_FUNC_DESC, arena);

      // Set the comparator
      return (int) mdb_set_compare.invoke(txn, dbi, stub);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_set_compare", t.getMessage(), t);
    }
  }

  /**
   * Set a custom duplicate data comparison function for a database.
   * Only meaningful for databases opened with MDB_DUPSORT flag.
   * Must be called before any data access operations.
   * The Arena must remain alive for the lifetime of the database.
   */
  public static int mdb_set_dupsort(MemorySegment txn, int dbi, ComparatorCallback comparator, Arena arena) {
    try {
      // Create a method handle that adapts the comparator
      MethodHandle adapterHandle = createComparatorAdapter(comparator);

      // Create upcall stub
      MemorySegment stub = LINKER.upcallStub(adapterHandle, MDB_CMP_FUNC_DESC, arena);

      // Set the comparator
      return (int) mdb_set_dupsort.invoke(txn, dbi, stub);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_set_dupsort", t.getMessage(), t);
    }
  }

  /**
   * Create a method handle adapter for the comparator
   */
  private static MethodHandle createComparatorAdapter(ComparatorCallback comparator) throws Throwable {
    // Create a lambda that wraps MemorySegment pointers as MDB_val
    java.util.function.BiFunction<MemorySegment, MemorySegment, Integer> adapter =
            (aPtr, bPtr) -> {
              MDB_val a = new MDB_val(aPtr.reinterpret(MDB_val.layout().byteSize()));
              MDB_val b = new MDB_val(bPtr.reinterpret(MDB_val.layout().byteSize()));
              return comparator.compare(a, b);
            };

    // Get method handle for the adapter
    return MethodHandles.lookup()
            .findVirtual(
                    java.util.function.BiFunction.class,
                    "apply",
                    MethodType.methodType(Object.class, Object.class, Object.class)
            )
            .bindTo(adapter)
            .asType(MethodType.methodType(
                    int.class, MemorySegment.class, MemorySegment.class
            ));
  }


  // Utility functions
  public static String mdb_strerror(int err) {
    try {
      MemorySegment result = (MemorySegment) mdb_strerror.invoke(err);
      return result.reinterpret(Long.MAX_VALUE).getString(0);
    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cmp", t.getMessage(), t);
    }
  }

  public static int mdb_reader_check(MemorySegment env, MemorySegment resultPtr) {
    try {
      return (int) mdb_reader_check.invoke(env, resultPtr);

    } catch (final Throwable t) {
      throw new LmdbInvokeException("mdb_cmp", t.getMessage(), t);
    }
  }

  private static Path extract(final String name) {
    final String suffix = name.substring(name.lastIndexOf('.'));
    final Path file;
    try {
      final Path dir = Paths.get(EXTRACT_DIR);
      if (!Files.exists(dir) || !Files.isDirectory(dir)) {
        throw new IllegalStateException("Invalid extraction directory " + dir);
      }
      file = Files.createTempFile(dir, "lmdbjava-native-library-", suffix);
      // Register for deletion on JVM exit
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          Files.deleteIfExists(file);
        } catch (IOException e) {
          // Log or ignore
        }
      }));

      final ClassLoader cl = currentThread().getContextClassLoader();
      try (InputStream in = cl.getResourceAsStream(name);
           OutputStream out = Files.newOutputStream(file)) {
        requireNonNull(in, "Classpath resource not found");
        int bytes;
        final byte[] buffer = new byte[4_096];
        while (-1 != (bytes = in.read(buffer))) {
          out.write(buffer, 0, bytes);
        }
      }
      return file;
    } catch (final IOException e) {
      throw new LmdbException("Failed to extract " + name, e);
    }
  }
}
