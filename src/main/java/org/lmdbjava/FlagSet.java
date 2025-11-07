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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A set of flags, each with a bit mask value. Flags can be combined in a set such that the set has
 * a combined bit mask value.
 *
 * @param <T> The type of flag in the set, must extend {@link MaskedFlag}.
 */
public interface FlagSet<T extends MaskedFlag> extends Iterable<T> {

  /**
   * The combined mask for this flagSet.
   *
   * @return The combined mask for this flagSet.
   */
  int getMask();

  /**
   * Combines this {@link FlagSet} with another and returns the combined mask value.
   *
   * @param other The other {@link FlagSet} to combine with this.
   * @return The result of combining the mask of this {@link FlagSet} with the mask of the other
   *     {@link FlagSet}.
   */
  default int getMaskWith(final FlagSet<T> other) {
    if (other != null) {
      return MaskedFlag.mask(getMask(), other.getMask());
    } else {
      return getMask();
    }
  }

  /**
   * Get the set of flags in this {@link FlagSet}.
   *
   * @return The set of flags in this {@link FlagSet}.
   */
  Set<T> getFlags();

  /**
   * Tests if flag is non-null and included in this {@link FlagSet}.
   *
   * @param flag The flag to test.
   * @return True if flag is non-null and included in this {@link FlagSet}.
   */
  boolean isSet(T flag);

  /**
   * The number of flags in this set.
   *
   * @return The number of flags in this set.
   */
  int size();

  /**
   * Tests if at least one of flags are included in this {@link FlagSet}
   *
   * @param flags The flags to test.
   * @return True if at least one of flags are included in this {@link FlagSet}
   */
  default boolean areAnySet(final FlagSet<T> flags) {
    if (flags == null) {
      return false;
    } else {
      for (final T flag : flags) {
        if (isSet(flag)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Tests if this {@link FlagSet} is empty.
   *
   * @return True if this {@link FlagSet} is empty.
   */
  boolean isEmpty();

  /**
   * Gets an {@link Iterator} (in no particular order) for the flags in this {@link FlagSet}.
   *
   * @return The {@link Iterator} (in no particular order) for the flags in this {@link FlagSet}.
   */
  @Override
  default Iterator<T> iterator() {
    return getFlags().iterator();
  }

  /**
   * Convert this {@link FlagSet} to a string for use in toString methods.
   *
   * @param flagSet The {@link FlagSet} to convert to a string.
   * @param <T> The type of the flags in the {@link FlagSet}.
   * @return The {@link String} representation of the flagSet.
   */
  static <T extends MaskedFlag> String asString(final FlagSet<T> flagSet) {
    Objects.requireNonNull(flagSet);
    final String flagsStr =
        flagSet.getFlags().stream()
            .sorted(Comparator.comparing(MaskedFlag::getMask))
            .map(MaskedFlag::name)
            .collect(Collectors.joining(", "));
    return "FlagSet{" + "flags=[" + flagsStr + "], mask=" + flagSet.getMask() + '}';
  }

  /**
   * Compares a {@link FlagSet} to another object
   *
   * @param flagSet The {@link FlagSet} to compare.
   * @param other THe object to compare against the {@link FlagSet}.
   * @return True if both arguments implement {@link FlagSet} and contain the same flags.
   */
  static boolean equals(final FlagSet<?> flagSet, final Object other) {
    if (other instanceof FlagSet) {
      final FlagSet<?> flagSet2 = (FlagSet<?>) other;
      if (flagSet == flagSet2) {
        return true;
      } else if (flagSet == null) {
        return false;
      } else {
        return flagSet.getMask() == flagSet2.getMask()
            && Objects.equals(flagSet.getFlags(), flagSet2.getFlags());
      }
    } else {
      return false;
    }
  }
}
