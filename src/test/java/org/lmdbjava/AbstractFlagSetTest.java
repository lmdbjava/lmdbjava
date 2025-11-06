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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public abstract class AbstractFlagSetTest<T extends Enum<T> & MaskedFlag & FlagSet<T>, F extends FlagSet<T>> {

  abstract List<T> getAllFlags();

  abstract F getEmptyFlagSet();

  abstract AbstractFlagSet.Builder<T, F> getBuilder();

  abstract F getFlagSet(final Collection<T> flags);

  abstract F getFlagSet(final T[] flags);

  abstract F getFlagSet(final T flag);

  abstract Class<T> getFlagType();

  T getFirst() {
    return getAllFlags().get(0);
  }

  @Test
  void testEmpty() {
    final F emptyFlagSet = getEmptyFlagSet();
    assertThat(emptyFlagSet.getMask())
        .isEqualTo(0);
    assertThat(emptyFlagSet.getFlags())
        .isEmpty();
    assertThat(emptyFlagSet.isEmpty())
        .isTrue();
    assertThat(emptyFlagSet.size())
        .isEqualTo(0);
    assertThat(emptyFlagSet.isSet(getFirst()))
        .isFalse();
    assertThat(getBuilder().build().getFlags())
        .isEqualTo(emptyFlagSet.getFlags());
  }

  @Test
  void testSingleFlagSet() {
    final List<T> allFlags = getAllFlags();
    for (T flag : allFlags) {
      final F flagSet = getBuilder()
          .addFlag(flag)
          .build();
      assertThat(flagSet.getMask())
          .isEqualTo(flag.getMask());
      assertThat(flagSet.getMask())
          .isEqualTo(MaskedFlag.mask(flag));
      assertThat(flagSet.getFlags())
          .containsExactly(flag);
      assertThat(flagSet.size())
          .isEqualTo(1);
      assertThat(FlagSet.equals(flagSet, new Object()))
          .isFalse();
      assertThat(FlagSet.equals(flagSet, null))
          .isFalse();
      assertThat(FlagSet.equals(flag, flag))
          .isTrue();
      assertThat(FlagSet.equals(flagSet, flag))
          .isTrue();
      assertThat(FlagSet.equals(flagSet, getFlagSet(flag)))
          .isTrue();
      assertThat(FlagSet.equals(flagSet, getFlagSet(flagSet.getFlags())))
          .isTrue();
      assertThat(flagSet.areAnySet(flag))
          .isTrue();
      assertThat(flagSet.areAnySet(null))
          .isFalse();
      assertThat(flagSet.areAnySet(getEmptyFlagSet()))
          .isFalse();
      assertThat(flagSet.isSet(getFirst()))
          .isEqualTo(getFirst() == flag);
      if (getFirst() == flag) {
        assertThat(flagSet.getMask())
            .isEqualTo(MaskedFlag.mask(getFirst()));
      } else {
        assertThat(flagSet.getMask())
            .isNotEqualTo(MaskedFlag.mask(getFirst()));
        assertThat(flagSet.getMaskWith(getFirst()))
            .isEqualTo(MaskedFlag.mask(flag, getFirst()));
      }
      assertThat(flagSet.toString())
          .isNotNull();
      assertThat(flag.name())
          .isNotNull();
      assertThat(flagSet.getMaskWith(null))
          .isEqualTo(flagSet.getMask());
    }
  }

  @Test
  void testAllFlags() {
    final List<T> allFlags = getAllFlags();
    final List<T> flags = new ArrayList<>(allFlags.size());
    final Set<Integer> masks = new HashSet<>();
    final T firstFlag = getFirst();
    for (T flag : allFlags) {
      flags.add(flag);
      final F flagSet = getBuilder()
          .setFlags(flags)
          .build();
      final int flagSetMask = flagSet.getMask();

      assertThat(masks)
          .doesNotContain(flagSetMask);
      masks.add(flagSetMask);
      assertThat(flagSetMask)
          .isEqualTo(MaskedFlag.mask(flags));
      final T[] flagsArr = flags.stream().toArray(this::toArray);
      assertThat(flagSetMask)
          .isEqualTo(MaskedFlag.mask(flagsArr));
      assertThat(flagSet.getFlags())
          .containsExactlyElementsOf(flags);
      assertThat(flagSet)
          .isNotEmpty();
      assertThat(FlagSet.equals(flagSet, getBuilder().setFlags(flagsArr).build()))
          .isTrue();
      assertThat(FlagSet.equals(flagSet, getFlagSet(flags)))
          .isTrue();
      assertThat(FlagSet.equals(flagSet, getFlagSet(flagsArr)))
          .isTrue();
      assertThat(flagSet.size())
          .isEqualTo(flags.size());
      assertThat(flagSet.isSet(getFirst()))
          .isEqualTo(true);

      final int maskWith = flagSet.getMaskWith(firstFlag);
      final List<T> combinedList = new ArrayList<>(flags);
      combinedList.add(firstFlag);
      assertThat(maskWith)
          .isEqualTo(MaskedFlag.mask(combinedList));
    }
  }

  private T[] toArray(final int cnt) {
    //noinspection unchecked
    return (T[]) Array.newInstance(getFlagType(), cnt);
  }
}
