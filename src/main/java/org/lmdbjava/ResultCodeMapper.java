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

import static jnr.constants.ConstantSet.getConstantSet;

import jnr.constants.Constant;
import jnr.constants.ConstantSet;
import org.lmdbjava.Txn.BadException;
import org.lmdbjava.Txn.BadReaderLockException;
import org.lmdbjava.Txn.TxFullException;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps a LMDB C result code to the equivalent Java exception.
 *
 * <p>The immutable nature of all LMDB exceptions means the mapper internally maintains a table of
 * them.
 */
final class ResultCodeMapper {

  /** Successful result. */
  static final int MDB_SUCCESS = 0;

  private static final String POSIX_ERR_NO = "Errno";
  private static final Map<Integer, RuntimeException> EXCEPTION_MAP = new HashMap<>();

  static {
    for (final Constant constant : getConstantSet(POSIX_ERR_NO)) {
      EXCEPTION_MAP.put(
              constant.intValue(),
              new LmdbNativeException.ConstantDerivedException(
                      constant.intValue(), constant.name() + " " + constant));
    }
    EXCEPTION_MAP.put( Dbi.BadDbiException.MDB_BAD_DBI, new Dbi.BadDbiException());
    EXCEPTION_MAP.put( BadReaderLockException.MDB_BAD_RSLOT, new BadReaderLockException());
    EXCEPTION_MAP.put( BadException.MDB_BAD_TXN, new BadException());
    EXCEPTION_MAP.put( Dbi.BadValueSizeException.MDB_BAD_VALSIZE, new Dbi.BadValueSizeException());
    EXCEPTION_MAP.put( LmdbNativeException.PageCorruptedException.MDB_CORRUPTED, new LmdbNativeException.PageCorruptedException());
    EXCEPTION_MAP.put( Cursor.FullException.MDB_CURSOR_FULL, new Cursor.FullException());
    EXCEPTION_MAP.put( Dbi.DbFullException.MDB_DBS_FULL, new Dbi.DbFullException());
    EXCEPTION_MAP.put( Dbi.IncompatibleException.MDB_INCOMPATIBLE, new Dbi.IncompatibleException());
    EXCEPTION_MAP.put( Env.FileInvalidException.MDB_INVALID, new Env.FileInvalidException());
    EXCEPTION_MAP.put( Dbi.KeyExistsException.MDB_KEYEXIST, new Dbi.KeyExistsException());
    EXCEPTION_MAP.put( Env.MapFullException.MDB_MAP_FULL,new Env.MapFullException());
    EXCEPTION_MAP.put( Dbi.MapResizedException.MDB_MAP_RESIZED, new Dbi.MapResizedException());
    EXCEPTION_MAP.put( Dbi.KeyNotFoundException.MDB_NOTFOUND, new Dbi.KeyNotFoundException());
    EXCEPTION_MAP.put( LmdbNativeException.PageFullException.MDB_PAGE_FULL, new LmdbNativeException.PageFullException());
    EXCEPTION_MAP.put( LmdbNativeException.PageNotFoundException.MDB_PAGE_NOTFOUND, new LmdbNativeException.PageNotFoundException());
    EXCEPTION_MAP.put( LmdbNativeException.PanicException.MDB_PANIC, new LmdbNativeException.PanicException());
    EXCEPTION_MAP.put( Env.ReadersFullException.MDB_READERS_FULL, new Env.ReadersFullException());
    EXCEPTION_MAP.put( LmdbNativeException.TlsFullException.MDB_TLS_FULL, new LmdbNativeException.TlsFullException());
    EXCEPTION_MAP.put( TxFullException.MDB_TXN_FULL, new TxFullException());
    EXCEPTION_MAP.put( Env.VersionMismatchException.MDB_VERSION_MISMATCH, new Env.VersionMismatchException());
  }

  private ResultCodeMapper() {}

  /**
   * Checks the result code and raises an exception is not {@link #MDB_SUCCESS}.
   *
   * @param rc the LMDB result code
   */
  static void checkRc(final int rc) {
    if (rc != MDB_SUCCESS) {
      final RuntimeException exception = EXCEPTION_MAP.get(rc);
      if (exception == null) {
        throw new IllegalArgumentException("Unknown result code " + rc);
      }
      throw exception;
    }
  }
}
