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

import static java.util.Objects.requireNonNull;

import java.util.Collection;

/** Indicates an enum that can provide integers for each of its values. */
public interface MaskedFlag {

  int EMPTY_MASK = 0;

  /**
   * Obtains the integer value for this enum which can be included in a mask.
   *
   * @return the integer value for combination into a mask
   */
  int getMask();

  /**
   * @return The name of the flag.
   */
  String name();

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param <M> flag type
   * @param flags to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
  @SafeVarargs
  static <M extends MaskedFlag> int mask(final M... flags) {
    if (flags == null || flags.length == 0) {
      return EMPTY_MASK;
    } else {
      int result = EMPTY_MASK;
      for (MaskedFlag flag : flags) {
        if (flag == null) {
          continue;
        }
        result |= flag.getMask();
      }
      return result;
    }
  }

  static <M extends MaskedFlag> int mask(final int mask1, final int mask2) {
    return mask1 | mask2;
  }

  static <M extends MaskedFlag> int mask(final Collection<M> flags) {
    if (flags == null || flags.isEmpty()) {
      return EMPTY_MASK;
    } else {
      int result = EMPTY_MASK;
      for (MaskedFlag flag : flags) {
        if (flag == null) {
          continue;
        }
        result |= flag.getMask();
      }
      return result;
    }
  }

  /**
   * Indicates whether the passed flag has the relevant masked flag high.
   *
   * @param flags to evaluate (usually produced by {@link #mask(org.lmdbjava.MaskedFlag...)}
   * @param test the flag being sought (required)
   * @return true if set.
   */
  static boolean isSet(final int flags, final MaskedFlag test) {
    requireNonNull(test);
    return (flags & test.getMask()) == test.getMask();
  }
}
