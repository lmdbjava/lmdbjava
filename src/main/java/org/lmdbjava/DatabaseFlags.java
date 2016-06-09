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

/**
 * Flags for use when opening a {@link Database}.
 */
public enum DatabaseFlags implements MaskedFlag {

  /**
   * use reverse string keys
   */
  MDB_REVERSEKEY(0x02),
  /**
   * use sorted duplicates
   */
  MDB_DUPSORT(0x04),
  /**
   * numeric keys in native byte order: either unsigned int or size_t. The keys
   * must all be of the same size.
   */
  MDB_INTEGERKEY(0x08),
  /**
   * with #MDB_DUPSORT, sorted dup items have fixed size
   */
  MDB_DUPFIXED(0x10),
  /**
   * with #MDB_DUPSORT, dups are #MDB_INTEGERKEY-style integers
   */
  MDB_INTEGERDUP(0x20),
  /**
   * with #MDB_DUPSORT, use reverse string dups
   */
  MDB_REVERSEDUP(0x40),
  /**
   * create DB if not already existing
   */
  MDB_CREATE(0x4_0000);

  private final int mask;

  DatabaseFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
