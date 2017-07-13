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
 * Key range type.
 *
 * <p>
 * In the examples below, it is assumed the table has keys 2, 4, 6 and 8.
 */
public enum KeyRangeType {

  /**
   * Starting on the first key and iterate forward until no keys remain.
   *
   * <p>
   * The "start" and "stop" values are ignored.
   *
   * <p>
   * In our example, the returned keys would be 2, 4, 6 and 8.
   */
  FORWARD(true, false, false),
  /**
   * Start on the passed key (or the first key immediately after it) and
   * iterate forward until no keys remain.
   *
   * <p>
   * The "start" value is required. The "stop" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 6 and 8. With a passed key of 6, the returned keys would be 6 and 8.
   */
  FORWARD_START(true, true, false),
  /**
   * Start on the first key and iterate forward until a key equal to it (or the
   * first key immediately after it) is reached.
   *
   * <p>
   * The "stop" value is required. The "start" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 2 and 4. With a passed key of 6, the returned keys would be 2, 4 and 6.
   */
  FORWARD_STOP(true, false, true),
  /**
   * Iterate forward between the passed keys, matching on the first keys
   * directly equal to the passed key (or immediately following it in the case
   * of the "start" key, or immediately preceding it in the case of the "stop"
   * key).
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 3 - 7, the returned keys
   * would be 4 and 6. With a range of 2 - 6, the keys would be 2, 4 and 6.
   */
  FORWARD_RANGE(true, true, true),
  /**
   * Start on the last key and iterate backward until no keys remain.
   *
   * <p>
   * The "start" and "stop" values are ignored.
   *
   * <p>
   * In our example, the returned keys would be 8, 6, 4 and 2.
   */
  BACKWARD(false, false, false),
  /**
   * Start on the passed key (or the first key immediately preceding it) and
   * iterate backward until no keys remain.
   *
   * <p>
   * The "start" value is required. The "stop" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 4 and 2. With a passed key of 6, the returned keys would be 6, 4 and 2.
   */
  BACKWARD_START(false, true, false),
  /**
   * Start on the last key and iterate backward until a key equal to it (or the
   * first key immediately preceding it it) is reached.
   *
   * <p>
   * The "stop" value is required. The "start" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 8 and 6. With a passed key of 6, the returned keys would be 8 and 6.
   */
  BACKWARD_STOP(false, false, true),
  /**
   * Iterate backward between the passed keys, matching on the first keys
   * directly equal to the passed key (or immediately preceding it in the case
   * of the "start" key, or immediately following it in the case of the "stop"
   * key).
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 7 - 3, the returned keys
   * would be 6 and 4. With a range of 6 - 2, the keys would be 6, 4 and 2.
   */
  BACKWARD_RANGE(false, true, true);

  private final boolean directionForward;
  private final boolean startKeyRequired;
  private final boolean stopKeyRequired;

  KeyRangeType(final boolean directionForward, final boolean startKeyRequired,
               final boolean stopKeyRequired) {
    this.directionForward = directionForward;
    this.startKeyRequired = startKeyRequired;
    this.stopKeyRequired = stopKeyRequired;
  }

  /**
   * Whether the key space is iterated in the order provided by LMDB.
   *
   * @return true if forward, false if reverse
   */
  public boolean isDirectionForward() {
    return directionForward;
  }

  /**
   * Whether the iteration requires a "start" key.
   *
   * @return true if start key must be non-null
   */
  public boolean isStartKeyRequired() {
    return startKeyRequired;
  }

  /**
   * Whether the iteration requires a "stop" key.
   *
   * @return true if stop key must be non-null
   */
  public boolean isStopKeyRequired() {
    return stopKeyRequired;
  }

}
