/*
 * Copyright 2016 LmdbJava
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

import static java.lang.String.format;

/**
 * Superclass for all exceptions that originate from a native C call.
 */
public abstract class LmdbNativeException extends LmdbException {

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
   *
   * @return the C-side result code
   */
  public final int getResultCode() {
    return rc;
  }
}
