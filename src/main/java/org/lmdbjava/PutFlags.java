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

import java.util.EnumSet;
import java.util.Set;

/** Flags for use when performing a "put". */
public enum PutFlags implements MaskedFlag, PutFlagSet {

  /** For put: Don't write if the key already exists. */
  MDB_NOOVERWRITE(0x10),
  /**
   * Only for #MDB_DUPSORT<br>
   * For put: don't write if the key and data pair already exist.<br>
   * For mdb_cursor_del: remove all duplicate data items.
   */
  MDB_NODUPDATA(0x20),
  /** For mdb_cursor_put: overwrite the current key/data pair. */
  MDB_CURRENT(0x40),
  /**
   * For put: Just reserve space for data, don't copy it. Return a pointer to the reserved space.
   */
  MDB_RESERVE(0x1_0000),
  /** Data is being appended, don't split full pages. */
  MDB_APPEND(0x2_0000),
  /** Duplicate data is being appended, don't split full pages. */
  MDB_APPENDDUP(0x4_0000),
  /** Store multiple data items in one call. Only for #MDB_DUPFIXED. */
  MDB_MULTIPLE(0x8_0000);

  private final int mask;

  PutFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

  @Override
  public Set<PutFlags> getFlags() {
    return EnumSet.of(this);
  }

  @Override
  public boolean isSet(PutFlags flag) {
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
