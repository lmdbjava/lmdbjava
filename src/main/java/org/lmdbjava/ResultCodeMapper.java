/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2019 The LmdbJava Open Source Project
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

import jnr.constants.Constant;
import jnr.constants.ConstantSet;
import static jnr.constants.ConstantSet.getConstantSet;
import org.lmdbjava.Txn.BadException;
import org.lmdbjava.Txn.BadReaderLockException;
import org.lmdbjava.Txn.TxFullException;

/**
 * Maps a LMDB C result code to the equivalent Java exception.
 *
 * <p>
 * The immutable nature of all LMDB exceptions means the mapper internally
 * maintains a table of them.
 */
@SuppressWarnings("PMD.CyclomaticComplexity")
final class ResultCodeMapper {

  /**
   * Successful result.
   */
  static final int MDB_SUCCESS = 0;

  private static final ConstantSet CONSTANTS;
  private static final String POSIX_ERR_NO = "Errno";

  static {
    CONSTANTS = getConstantSet(POSIX_ERR_NO);
  }

  private ResultCodeMapper() {
  }

  /**
   * Checks the result code and raises an exception is not {@link #MDB_SUCCESS}.
   *
   * @param rc the LMDB result code
   * @throws LmdbNativeException the resolved exception
   */
  static void checkRc(final int rc) throws LmdbNativeException {
    switch (rc) {
      case MDB_SUCCESS:
        return;
      case Dbi.BadDbiException.MDB_BAD_DBI:
        throw new Dbi.BadDbiException();
      case BadReaderLockException.MDB_BAD_RSLOT:
        throw new BadReaderLockException();
      case BadException.MDB_BAD_TXN:
        throw new BadException();
      case Dbi.BadValueSizeException.MDB_BAD_VALSIZE:
        throw new Dbi.BadValueSizeException();
      case LmdbNativeException.PageCorruptedException.MDB_CORRUPTED:
        throw new LmdbNativeException.PageCorruptedException();
      case Cursor.FullException.MDB_CURSOR_FULL:
        throw new Cursor.FullException();
      case Dbi.DbFullException.MDB_DBS_FULL:
        throw new Dbi.DbFullException();
      case Dbi.IncompatibleException.MDB_INCOMPATIBLE:
        throw new Dbi.IncompatibleException();
      case Env.FileInvalidException.MDB_INVALID:
        throw new Env.FileInvalidException();
      case Dbi.KeyExistsException.MDB_KEYEXIST:
        throw new Dbi.KeyExistsException();
      case Env.MapFullException.MDB_MAP_FULL:
        throw new Env.MapFullException();
      case Dbi.MapResizedException.MDB_MAP_RESIZED:
        throw new Dbi.MapResizedException();
      case Dbi.KeyNotFoundException.MDB_NOTFOUND:
        throw new Dbi.KeyNotFoundException();
      case LmdbNativeException.PageFullException.MDB_PAGE_FULL:
        throw new LmdbNativeException.PageFullException();
      case LmdbNativeException.PageNotFoundException.MDB_PAGE_NOTFOUND:
        throw new LmdbNativeException.PageNotFoundException();
      case LmdbNativeException.PanicException.MDB_PANIC:
        throw new LmdbNativeException.PanicException();
      case Env.ReadersFullException.MDB_READERS_FULL:
        throw new Env.ReadersFullException();
      case LmdbNativeException.TlsFullException.MDB_TLS_FULL:
        throw new LmdbNativeException.TlsFullException();
      case TxFullException.MDB_TXN_FULL:
        throw new TxFullException();
      case Env.VersionMismatchException.MDB_VERSION_MISMATCH:
        throw new Env.VersionMismatchException();
      default:
        break;
    }

    final Constant constant = CONSTANTS.getConstant(rc);
    if (constant == null) {
      throw new IllegalArgumentException("Unknown result code " + rc);
    }
    final String msg = constant.name() + " " + constant.toString();
    throw new LmdbNativeException.ConstantDerviedException(rc, msg);
  }

}
