/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2021 The LmdbJava Open Source Project
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

import static java.lang.String.format;

/**
 * Superclass for all exceptions that originate from a native C call.
 */
public class LmdbNativeException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Result code returned by the LMDB C function.
   */
  private final int rc;

  /**
   * Constructs an instance with the provided detailed message.
   *
   * @param msg the detail message.
   * @param rc  the result code.
   */
  LmdbNativeException(final int rc, final String msg) {
    super(format(msg + " (%d)", rc));
    this.rc = rc;
  }

  /**
   * Obtain the LMDB C-side result code.
   *
   * @return the C-side result code
   */
  public final int getResultCode() {
    return rc;
  }

  /**
   * Exception raised from a system constant table lookup.
   */
  public static final class ConstantDerivedException extends LmdbNativeException {

    private static final long serialVersionUID = 1L;

    ConstantDerivedException(final int rc, final String message) {
      super(rc, "Platform constant error code: " + message);
    }
  }

  /**
   * Located page was wrong type.
   */
  public static final class PageCorruptedException extends LmdbNativeException {

    static final int MDB_CORRUPTED = -30_796;
    private static final long serialVersionUID = 1L;

    PageCorruptedException() {
      super(MDB_CORRUPTED, "located page was wrong type");
    }
  }

  /**
   * Page has not enough space - internal error.
   */
  public static final class PageFullException extends LmdbNativeException {

    static final int MDB_PAGE_FULL = -30_786;
    private static final long serialVersionUID = 1L;

    PageFullException() {
      super(MDB_PAGE_FULL, "Page has not enough space - internal error");
    }
  }

  /**
   * Requested page not found - this usually indicates corruption.
   */
  public static final class PageNotFoundException extends LmdbNativeException {

    static final int MDB_PAGE_NOTFOUND = -30_797;
    private static final long serialVersionUID = 1L;

    PageNotFoundException() {
      super(MDB_PAGE_NOTFOUND,
            "Requested page not found - this usually indicates corruption");
    }
  }

  /**
   * Update of meta page failed or environment had fatal error.
   */
  public static final class PanicException extends LmdbNativeException {

    static final int MDB_PANIC = -30_795;
    private static final long serialVersionUID = 1L;

    PanicException() {
      super(MDB_PANIC,
            "Update of meta page failed or environment had fatal error");
    }
  }

  /**
   * Too many TLS keys in use - Windows only.
   */
  public static final class TlsFullException extends LmdbNativeException {

    static final int MDB_TLS_FULL = -30_789;
    private static final long serialVersionUID = 1L;

    TlsFullException() {
      super(MDB_TLS_FULL, "Too many TLS keys in use - Windows only");
    }
  }
}
