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

/** For comparing a cursor's current key against a {@link KeyRange}'s start/stop key. */
interface RangeComparator extends AutoCloseable {

  /**
   * Compare the cursor's current key to the range start key. Equivalent to compareTo(currentKey,
   * startKey)
   */
  int compareToStartKey();

  /**
   * Compare the cursor's current key to the range stop key. Equivalent to compareTo(currentKey,
   * stopKey)
   */
  int compareToStopKey();
}
