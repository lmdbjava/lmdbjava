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
 * A set of flags, each with a bit mask value.
 * Flags can be combined in a set such that the set has a combined bit mask value.
 * @param <T>
 */
public interface FlagSet<T extends MaskedFlag> extends Iterable<T> {

  int getMask();

  Set<T> getFlags();

  boolean isSet(T flag);

  default int size() {
    return getFlags().size();
  }

  default boolean isEmpty() {
    return getFlags().isEmpty();
  }

  default Iterator<T> iterator() {
    return getFlags().iterator();
  }

  static <T extends MaskedFlag> String asString(final FlagSet<T> flagSet) {
    Objects.requireNonNull(flagSet);
    final String flagsStr = flagSet.getFlags()
        .stream()
        .sorted(Comparator.comparing(MaskedFlag::getMask))
        .map(MaskedFlag::name)
        .collect(Collectors.joining(", "));
    return "FlagSet{" +
        "flags=[" + flagsStr +
        "], mask=" + flagSet.getMask() +
        '}';
  }

  static boolean equals(final FlagSet<?> flagSet1,
                        final FlagSet<?> flagSet2) {
    if (flagSet1 == flagSet2) {
      return true;
    } else if (flagSet1 != null && flagSet2 == null) {
      return false;
    } else if (flagSet1 == null) {
      return false;
    } else {
      return flagSet1.getMask() == flagSet2.getMask()
          && Objects.equals(flagSet1.getFlags(), flagSet2.getFlags());
    }
  }

}
