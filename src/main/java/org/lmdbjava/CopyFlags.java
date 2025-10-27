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

import java.io.File;
import java.util.EnumSet;
import java.util.Set;

/** Flags for use when performing a {@link Env#copy(File, CopyFlagSet)}. */
public enum CopyFlags implements MaskedFlag, CopyFlagSet {

  /** Compacting copy: Omit free space from copy, and renumber all pages sequentially. */
  MDB_CP_COMPACT(0x01);

  private final int mask;

  CopyFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

  @Override
  public Set<CopyFlags> getFlags() {
    return EnumSet.of(this);
  }

  @Override
  public boolean isSet(final CopyFlags flag) {
    return flag != null && mask == flag.getMask();
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public String toString() {
    return FlagSet.asString(this);
  }
}
