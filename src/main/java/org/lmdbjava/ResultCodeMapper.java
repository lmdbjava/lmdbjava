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

import jnr.constants.Constant;
import jnr.constants.ConstantSet;
import static jnr.constants.ConstantSet.getConstantSet;
import static org.lmdbjava.CursorFullException.MDB_CURSOR_FULL;
import static org.lmdbjava.Database.BadDbiException.MDB_BAD_DBI;
import static org.lmdbjava.Database.BadValueSizeException.MDB_BAD_VALSIZE;
import static org.lmdbjava.Database.IncompatibleException.MDB_INCOMPATIBLE;
import static org.lmdbjava.Database.KeyExistsException.MDB_KEYEXIST;
import static org.lmdbjava.Database.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Database.MapResizedException.MDB_MAP_RESIZED;
import static org.lmdbjava.DatabasesFullException.MDB_DBS_FULL;
import static org.lmdbjava.EnvMapFullException.MDB_MAP_FULL;
import static org.lmdbjava.EnvReadersFullException.MDB_READERS_FULL;
import static org.lmdbjava.EnvVersionMismatchException.MDB_VERSION_MISMATCH;
import static org.lmdbjava.FileInvalidException.MDB_INVALID;
import static org.lmdbjava.PageCorruptedException.MDB_CORRUPTED;
import static org.lmdbjava.PageFullException.MDB_PAGE_FULL;
import static org.lmdbjava.PageNotFoundException.MDB_PAGE_NOTFOUND;
import static org.lmdbjava.PanicException.MDB_PANIC;
import static org.lmdbjava.TlsFullException.MDB_TLS_FULL;
import static org.lmdbjava.TxnBadException.MDB_BAD_TXN;
import static org.lmdbjava.TxnBadReaderLockTableSlotException.MDB_BAD_RSLOT;
import static org.lmdbjava.TxnFullException.MDB_TXN_FULL;

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
        return new org.lmdbjava.Database.BadDbiException();
      case MDB_BAD_RSLOT:
        return new TxnBadReaderLockTableSlotException();
      case MDB_BAD_TXN:
        return new TxnBadException();
      case MDB_BAD_VALSIZE:
        return new org.lmdbjava.Database.BadValueSizeException();
      case MDB_CORRUPTED:
        return new PageCorruptedException();
      case MDB_CURSOR_FULL:
        return new CursorFullException();
      case MDB_DBS_FULL:
        return new DatabasesFullException();
      case MDB_INCOMPATIBLE:
        return new Database.IncompatibleException();
      case MDB_INVALID:
        return new FileInvalidException();
      case MDB_KEYEXIST:
        return new org.lmdbjava.Database.KeyExistsException();
      case MDB_MAP_FULL:
        return new EnvMapFullException();
      case MDB_MAP_RESIZED:
        return new org.lmdbjava.Database.MapResizedException();
      case MDB_NOTFOUND:
        return new org.lmdbjava.Database.KeyNotFoundException();
      case MDB_PAGE_FULL:
        return new PageFullException();
      case MDB_PAGE_NOTFOUND:
        return new PageNotFoundException();
      case MDB_PANIC:
        return new PanicException();
      case MDB_READERS_FULL:
        return new EnvReadersFullException();
      case MDB_TLS_FULL:
        return new TlsFullException();
      case MDB_TXN_FULL:
        return new TxnFullException();
      case MDB_VERSION_MISMATCH:
        return new EnvVersionMismatchException();
    }
    return null;
  }

  private ResultCodeMapper() {
  }
}
