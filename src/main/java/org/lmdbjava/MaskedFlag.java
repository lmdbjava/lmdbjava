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

import static java.util.Objects.requireNonNull;

/**
 * Indicates an enum that can provide integers for each of its values,
 */
public interface MaskedFlag {

  /**
   * Obtains the integer value for this enum which can be included in a mask.
   *
   * @return the integer value for combination into a mask
   */
  int getMask();

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param flags to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
  static int mask(final MaskedFlag... flags) {
    if (flags == null || flags.length == 0) {
      return 0;
    }

    int result = 0;
    for (MaskedFlag flag : flags) {
      if (flag == null) {
        continue;
      }
      result |= flag.getMask();
    }
    return result;
  }

  /**
   * Indicates whether the passed flag has the relevant masked flag high.
   *
   * @param flags to evaluate (usually produced by
   *              {@link #mask(org.lmdbjava.MaskedFlag...)}
   * @param test  the flag being sought (required)
   * @return true if set.
   */
  static boolean isSet(final int flags, final MaskedFlag test) {
    requireNonNull(test);
    return (flags & test.getMask()) == test.getMask();
  }
}
