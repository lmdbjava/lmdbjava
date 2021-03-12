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

/**
 * Flags for use when opening a {@link Dbi}.
 */
public enum DbiFlags implements MaskedFlag {

  /**
   * Use reverse string keys.
   *
   * <p>
   * Keys are strings to be compared in reverse order, from the end of the
   * strings to the beginning. By default, keys are treated as strings and
   * compared from beginning to end.
   */
  MDB_REVERSEKEY(0x02),
  /**
   * Use sorted duplicates.
   *
   * <p>
   * Duplicate keys may be used in the database. Or, from another perspective,
   * keys may have multiple data items, stored in sorted order. By default keys
   * must be unique and may have only a single data item.
   */
  MDB_DUPSORT(0x04),
  /**
   * Numeric keys in native byte order: either unsigned int or size_t. The keys
   * must all be of the same size.
   */
  MDB_INTEGERKEY(0x08),
  /**
   * With {@link #MDB_DUPSORT}, sorted dup items have fixed size.
   *
   * <p>
   * This flag may only be used in combination with {@link #MDB_DUPSORT}. This
   * option tells the library that the data items for this database are all the
   * same size, which allows further optimizations in storage and retrieval.
   * When all data items are the same size, the {@link SeekOp#MDB_GET_MULTIPLE}
   * and {@link SeekOp#MDB_NEXT_MULTIPLE} cursor operations may be used to
   * retrieve multiple items at once.
   */
  MDB_DUPFIXED(0x10),
  /**
   * With {@link #MDB_DUPSORT}, dups are {@link #MDB_INTEGERKEY}-style integers.
   *
   * <p>
   * This option specifies that duplicate data items are binary integers,
   * similar to {@link #MDB_INTEGERKEY} keys.
   */
  MDB_INTEGERDUP(0x20),
  /**
   * With {@link #MDB_DUPSORT}, use reverse string dups.
   *
   * <p>
   * This option specifies that duplicate data items should be compared as
   * strings in reverse order.
   */
  MDB_REVERSEDUP(0x40),
  /**
   * Create the named database if it doesn't exist.
   *
   * <p>
   * This option is not allowed in a read-only transaction or a read-only
   * environment.
   */
  MDB_CREATE(0x4_0000);

  private final int mask;

  DbiFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
