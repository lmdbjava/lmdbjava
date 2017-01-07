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
 * Statistics, as returned by {@link Env#stat()} and
 * {@link Dbi#stat(org.lmdbjava.Txn)}.
 */
public final class Stat {

  /**
   * Number of internal (non-leaf) pages.
   */
  public final long branchPages;

  /**
   * Depth (height) of the B-tree.
   */
  public final int depth;

  /**
   * Number of data items.
   */
  public final long entries;

  /**
   * Number of leaf pages.
   */
  public final long leafPages;

  /**
   * Number of overflow pages.
   */
  public final long overflowPages;

  /**
   * Size of a database page. This is currently the same for all databases.
   */
  public final int pageSize;

  Stat(final int pageSize, final int depth, final long branchPages,
       final long leafPages,
       final long overflowPages, final long entries) {
    this.pageSize = pageSize;
    this.depth = depth;
    this.branchPages = branchPages;
    this.leafPages = leafPages;
    this.overflowPages = overflowPages;
    this.entries = entries;
  }

}
