package org.lmdbjava;

import jnr.constants.Constant;
import jnr.constants.ConstantSet;
import static jnr.constants.ConstantSet.getConstantSet;
import static org.lmdbjava.BadDatabaseIdException.MDB_BAD_DBI;
import static org.lmdbjava.BadReaderLockTableSlotException.MDB_BAD_RSLOT;
import static org.lmdbjava.BadTransactionException.MDB_BAD_TXN;
import static org.lmdbjava.BadValueSizeException.MDB_BAD_VALSIZE;
import static org.lmdbjava.CorruptedException.MDB_CORRUPTED;
import static org.lmdbjava.CursorFullException.MDB_CURSOR_FULL;
import static org.lmdbjava.DatabasesFullException.MDB_DBS_FULL;
import static org.lmdbjava.IncompatibleException.MDB_INCOMPATIBLE;
import static org.lmdbjava.InvalidException.MDB_INVALID;
import static org.lmdbjava.KeyExistsException.MDB_KEYEXIST;
import static org.lmdbjava.MapFullException.MDB_MAP_FULL;
import static org.lmdbjava.MapResizedException.MDB_MAP_RESIZED;
import static org.lmdbjava.NotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.PageFullException.MDB_PAGE_FULL;
import static org.lmdbjava.PageNotFoundException.MDB_PAGE_NOTFOUND;
import static org.lmdbjava.PanicException.MDB_PANIC;
import static org.lmdbjava.ReadersFullException.MDB_READERS_FULL;
import static org.lmdbjava.TlsFullException.MDB_TLS_FULL;
import static org.lmdbjava.TransactionFullException.MDB_TXN_FULL;
import static org.lmdbjava.VersionMismatchException.MDB_VERSION_MISMATCH;

/**
 * Maps a LMDB C result code to the equivalent Java exception.
 */
public final class ResultCodeMapper {

  /**
   * Successful result
   */
  public static final int MDB_SUCCESS = 0;

  private static final ConstantSet CONSTANTS;
  private static final String POSIX_ERR_NO = "Errno";

  static {
    CONSTANTS = getConstantSet(POSIX_ERR_NO);
  }

  /**
   * Checks the result code and raises an exception is not {@link #MDB_SUCCESS}.
   *
   * @param rc the LMDB result code
   * @throws org.lmdbjava.LmdbNativeException
   */
  public static void checkRc(final int rc) throws LmdbNativeException {
    if (rc == MDB_SUCCESS) {
      return;
    }

    final LmdbNativeException nativeException = rcException(rc);
    if (nativeException != null) {
      throw nativeException;
    }

    final Constant constant = CONSTANTS.getConstant(rc);
    if (constant == null) {
      throw new IllegalArgumentException("Unknown result code " + rc);
    }
    throw new ConstantDerviedException(rc, constant.name());
  }

  /**
   * Returns the appropriate exception for a given result code.
   * <p>
   * The passed result code must be a value other than {@link #MDB_SUCCESS}.
   * Passing {@link #MDB_SUCCESS} will raise an exception.
   * <p>
   * If the passed result code cannot be mapped to an LMDB exception, null is
   * returned.
   *
   * @param rc the non-zero LMDB result code
   * @return the exception (may be null if not an LMDB result code)
   */
  static LmdbNativeException rcException(final int rc) throws
      IllegalArgumentException {
    if (rc == MDB_SUCCESS) {
      throw new IllegalArgumentException("Non-zero value required");
    }

    switch (rc) {
      case MDB_BAD_DBI:
        return new BadDatabaseIdException();
      case MDB_BAD_RSLOT:
        return new BadReaderLockTableSlotException();
      case MDB_BAD_TXN:
        return new BadTransactionException();
      case MDB_BAD_VALSIZE:
        return new BadValueSizeException();
      case MDB_CORRUPTED:
        return new CorruptedException();
      case MDB_CURSOR_FULL:
        return new CursorFullException();
      case MDB_DBS_FULL:
        return new DatabasesFullException();
      case MDB_INCOMPATIBLE:
        return new IncompatibleException();
      case MDB_INVALID:
        return new InvalidException();
      case MDB_KEYEXIST:
        return new KeyExistsException();
      case MDB_MAP_FULL:
        return new MapFullException();
      case MDB_MAP_RESIZED:
        return new MapResizedException();
      case MDB_NOTFOUND:
        return new NotFoundException();
      case MDB_PAGE_FULL:
        return new PageFullException();
      case MDB_PAGE_NOTFOUND:
        return new PageNotFoundException();
      case MDB_PANIC:
        return new PanicException();
      case MDB_READERS_FULL:
        return new ReadersFullException();
      case MDB_TLS_FULL:
        return new TlsFullException();
      case MDB_TXN_FULL:
        return new TransactionFullException();
      case MDB_VERSION_MISMATCH:
        return new VersionMismatchException();
    }
    return null;
  }

  private ResultCodeMapper() {
  }
}
