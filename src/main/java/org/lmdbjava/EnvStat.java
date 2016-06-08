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

/**
 * Environment statistics.
 */
public class EnvStat {

  public final long branchPages;
  public final int depth;
  public final long entries;
  public final long leafPages;
  public final long overflowPages;
  public final int pageSize;

  EnvStat(int pageSize, int depth, long branchPages, long leafPages,
          long overflowPages, long entries) {
    this.pageSize = pageSize;
    this.depth = depth;
    this.branchPages = branchPages;
    this.leafPages = leafPages;
    this.overflowPages = overflowPages;
    this.entries = entries;
  }

}
