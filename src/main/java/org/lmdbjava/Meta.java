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

import jnr.ffi.byref.IntByReference;
import static org.lmdbjava.Library.LIB;

/**
 * LMDB metadata functions.
 */
public final class Meta {

  private Meta() {
  }

  /**
   * Fetches the LMDB error code description.
   *
   * <p>
   * End users should not need this method, as LmdbJava converts all LMDB
   * exceptions into a typed Java exception that incorporates the error code.
   * However it is provided here for verification and troubleshooting (eg if the
   * user wishes to see the original LMDB description of the error code, or
   * there is a newer library version etc).
   *
   * @param err the error code returned from LMDB
   * @return the description
   */
  public static String error(final int err) {
    return LIB.mdb_strerror(err);
  }

  /**
   * Obtains the LMDB C library version information.
   *
   * @return the version data
   */
  public static Version version() {
    final IntByReference major = new IntByReference();
    final IntByReference minor = new IntByReference();
    final IntByReference patch = new IntByReference();

    LIB.mdb_version(major, minor, patch);

    return new Version(major.intValue(), minor.intValue(), patch.
                       intValue());
  }

  /**
   * Immutable return value from {@link #version()}.
   */
  public static final class Version {

    /**
     * LMDC native library major version number.
     */
    public final int major;

    /**
     * LMDC native library patch version number.
     */
    public final int minor;

    /**
     * LMDC native library patch version number.
     */
    public final int patch;

    Version(final int major, final int minor, final int patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }
  }

}
