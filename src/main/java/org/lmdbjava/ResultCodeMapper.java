/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import jnr.constants.Constant;
import jnr.constants.ConstantSet;
import static jnr.constants.ConstantSet.getConstantSet;
import org.lmdbjava.Txn.BadException;
import org.lmdbjava.Txn.BadReaderLockException;
import org.lmdbjava.Txn.TxFullException;

/**
 * Maps a LMDB C result code to the equivalent Java exception.
 * <p>
 * The immutable nature of all LMDB exceptions means the mapper internally
 * maintains a table of them.
 */
final class ResultCodeMapper {

  private static final ConstantSet CONSTANTS;
  private static final Map<Integer, LmdbNativeException> EXCEPTIONS;
  private static final String POSIX_ERR_NO = "Errno";

  /**
   * Successful result
   */
  static final int MDB_SUCCESS = 0;

  static {
    CONSTANTS = getConstantSet(POSIX_ERR_NO);
    EXCEPTIONS = new ConcurrentHashMap<>(20);
    add(new Dbi.BadDbiException());
    add(new BadReaderLockException());
    add(new BadException());
    add(new Dbi.BadValueSizeException());
    add(new LmdbNativeException.PageCorruptedException());
    add(new Cursor.FullException());
    add(new Dbi.DbFullException());
    add(new Dbi.IncompatibleException());
    add(new Env.FileInvalidException());
    add(new Dbi.KeyExistsException());
    add(new Env.MapFullException());
    add(new Dbi.MapResizedException());
    add(new Dbi.KeyNotFoundException());
    add(new LmdbNativeException.PageFullException());
    add(new LmdbNativeException.PageNotFoundException());
    add(new LmdbNativeException.PanicException());
    add(new Env.ReadersFullException());
    add(new LmdbNativeException.TlsFullException());
    add(new TxFullException());
    add(new Env.VersionMismatchException());
  }

  @SuppressWarnings("ThrowableResultIgnored")
  private static void add(final LmdbNativeException e) {
    EXCEPTIONS.put(e.getResultCode(), e);
  }

  /**
   * Checks the result code and raises an exception is not {@link #MDB_SUCCESS}.
   *
   * @param rc the LMDB result code
   * @throws LmdbNativeException the resolved exception
   */
  static void checkRc(final int rc) throws LmdbNativeException {
    if (rc == MDB_SUCCESS) {
      return;
    }

    final LmdbNativeException nativeException = EXCEPTIONS.get(rc);
    if (nativeException != null) {
      throw nativeException;
    }

    final Constant constant = CONSTANTS.getConstant(rc);
    if (constant == null) {
      throw new IllegalArgumentException("Unknown result code " + rc);
    }
    throw new LmdbNativeException.ConstantDerviedException(rc, constant.name());
  }

  private ResultCodeMapper() {
  }
}
