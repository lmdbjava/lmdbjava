package org.lmdbjava.core.lli;

import jnr.constants.Constant;
import jnr.constants.ConstantSet;

import static jnr.constants.ConstantSet.getConstantSet;

public class ReturnCodes {

  /**
   * Successful result
   */
  public static final int MDB_SUCCESS = 0;

  /**
   * key/data pair already exists
   */
  public static final int MDB_KEYEXIST = -30799;

  /**
   * key/data pair not found (EOF)
   */
  public static final int MDB_NOTFOUND = -30798;

  /**
   */
  public static final int MDB_PAGE_NOTFOUND = -30797;

  /**
   * Located page was wrong type
   */
  public static final int MDB_CORRUPTED = -30796;

  /**
   * Located page was wrong type
   */
  public static final int MDB_PANIC = -30795;

  /**
   * Environment version mismatch
   */
  public static final int MDB_VERSION_MISMATCH = -30794;

  /**
   * File is not a valid LMDB file
   */
  public static final int MDB_INVALID = -30793;

  /**
   * Environment mapsize reached
   */
  public static final int MDB_MAP_FULL = -30792;

  /**
   * Environment maxdbs reached
   */
  public static final int MDB_DBS_FULL = -30791;

  /**
   * Environment maxreaders reached
   */
  public static final int MDB_READERS_FULL = -30790;

  /**
   * Too many TLS keys in use - Windows only
   */
  public static final int MDB_TLS_FULL = -30789;

  /**
   * Txn has too many dirty pages
   */
  public static final int MDB_TXN_FULL = -30788;

  /**
   * Cursor stack too deep - internal error
   */
  public static final int MDB_CURSOR_FULL = -30787;

  /**
   * Page has not enough space - internal error
   */
  public static final int MDB_PAGE_FULL = -30786;

  /**
   * Database contents grew beyond environment mapsize
   */
  public static final int MDB_MAP_RESIZED = -30785;

  /**
   * Operation and DB incompatible, or DB type changed. This can mean:
   * The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.
   * Opening a named DB when the unnamed DB has MDB_DUPSORT / MDB_INTEGERKEY.
   * Accessing a data record as a database, or vice versa.
   * The database was dropped and recreated with different flags.
   */
  public static final int MDB_INCOMPATIBLE = -30784;

  /**
   * Invalid reuse of reader locktable slot
   */
  public static final int MDB_BAD_RSLOT = -30783;

  /**
   * Transaction must abort, has a child, or is invalid
   */
  public static final int MDB_BAD_TXN = -30782;

  /**
   * Unsupported size of key/DB name/data, or wrong DUPFIXED size
   */
  public static final int MDB_BAD_VALSIZE = -30781;

  /**
   * The specified DBI was changed unexpectedly
   */
  public static final int MDB_BAD_DBI = -30780;


  private static final ConstantSet CONSTANTS;
  private static final String POSIX_ERR_NO = "Errno";

  static {
    CONSTANTS = getConstantSet(POSIX_ERR_NO);
  }

  /**
   * Checks the return code and raises an exception is not {@link #MDB_SUCCESS}.
   *
   * @param rc the LMDB return code
   * @throws LmdbNativeException
   */
  static void checkRc(final int rc) throws LmdbNativeException {
    if (rc == MDB_SUCCESS) {
      return;
    } else if (rc > 0) {
      final Constant constant = CONSTANTS.getConstant(rc);
      if (constant == null) {
        throw new IllegalArgumentException("Unknown result code " + rc);
      }
      throw new ConstantDerviedException(rc, constant.name());
    } else {
      throw new LmdbNativeException(rc);
    }
  }

  /**
   * Returns the appropriate exception for a given return code.
   * <p>
   * The passed return code must be a value other than {@link #MDB_SUCCESS}.
   * Passing {@link #MDB_SUCCESS} will raise an exception.
   * <p>
   * If the passed return code cannot be mapped to an LMDB exception, null is
   * returned.
   *
   * @param rc the non-zero LMDB return code
   * @return the exception (may be null if not an LMDB return code)
   */
  static LmdbNativeException rcException(final int rc) throws
    IllegalArgumentException {
    if (rc == MDB_SUCCESS) {
      throw new IllegalArgumentException("Non-zero value required");
    }
    return new LmdbNativeException(rc);
  }
}
