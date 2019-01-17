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

/**
 * Flags for use when performing a
 * {@link Cursor#get(java.lang.Object, org.lmdbjava.GetOp)}.
 *
 * <p>
 * Unlike most other LMDB enums, this enum is not bit masked.
 */
public enum GetOp {

  /**
   * Position at specified key.
   */
  MDB_SET(15),
  /**
   * Position at specified key, return key + data.
   */
  MDB_SET_KEY(16),
  /**
   * Position at first key greater than or equal to specified key.
   */
  MDB_SET_RANGE(17);

  private final int code;

  GetOp(final int code) {
    this.code = code;
  }

  /**
   * Obtain the integer code for use by LMDB C API.
   *
   * @return the code
   */
  public int getCode() {
    return code;
  }

}
