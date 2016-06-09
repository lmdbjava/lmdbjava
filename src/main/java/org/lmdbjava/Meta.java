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

import jnr.ffi.byref.IntByReference;
import static org.lmdbjava.Library.lib;

/**
 * LMDB metadata functions.
 */
public final class Meta {

  /**
   * Obtains the LMDB C library version information.
   *
   * @return the version data
   */
  public static Version version() {
    final IntByReference major = new IntByReference();
    final IntByReference minor = new IntByReference();
    final IntByReference patch = new IntByReference();

    lib.mdb_version(major, minor, patch);

    return new Version(major.intValue(), minor.intValue(), patch.
                       intValue());
  }

  private Meta() {
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

    Version(int major, int minor, int patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }
  }

}
