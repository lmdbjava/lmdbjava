/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
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
 * {@link Env#copy(java.io.File, org.lmdbjava.CopyFlags...)}.
 */
public enum CopyFlags implements MaskedFlag {

  /**
   * Compacting copy: Omit free space from copy, and renumber all pages
   * sequentially.
   */
  MDB_CP_COMPACT(0x01);

  private final int mask;

  CopyFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
