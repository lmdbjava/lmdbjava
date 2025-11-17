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

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class PutFlagSetTest extends AbstractFlagSetTest<PutFlags, PutFlagSet> {

  @Test
  void test() {
    // This is here purely to stop CodeQL moaning that this class is unused.
    // All the actual tests are in the superclass
    Assertions.assertThat(getAllFlags()).isNotNull();
  }

  @Override
  List<PutFlags> getAllFlags() {
    return Arrays.stream(PutFlags.values()).collect(Collectors.toList());
  }

  @Override
  PutFlagSet getEmptyFlagSet() {
    return PutFlagSet.empty();
  }

  @Override
  AbstractFlagSet.Builder<PutFlags, PutFlagSet> getBuilder() {
    return PutFlagSet.builder();
  }

  @Override
  PutFlagSet getFlagSet(Collection<PutFlags> flags) {
    return PutFlagSet.of(flags);
  }

  @Override
  PutFlagSet getFlagSet(PutFlags[] flags) {
    return PutFlagSet.of(flags);
  }

  @Override
  PutFlagSet getFlagSet(PutFlags flag) {
    return PutFlagSet.of(flag);
  }

  @Override
  Class<PutFlags> getFlagType() {
    return PutFlags.class;
  }

  @Override
  Function<EnumSet<PutFlags>, PutFlagSet> getConstructor() {
    return PutFlagSetImpl::new;
  }

  /**
   * {@link FlagSet#isSet(MaskedFlag)} on the flag enum is tested in {@link AbstractFlagSetTest} but the coverage check
   * doesn't seem to notice it.
   */
  @Test
  void testIsSet() {
    assertThat(PutFlags.MDB_APPEND.isSet(PutFlags.MDB_APPEND))
        .isTrue();
    assertThat(PutFlags.MDB_APPEND.isSet(PutFlags.MDB_MULTIPLE))
        .isFalse();
    //noinspection ConstantValue
    assertThat(PutFlags.MDB_APPEND.isSet(null))
        .isFalse();
  }

  @Test
  public void testAddFlagVsCheckPresence() {

    final int cnt = 10_000_000;
    final int[] arr = new int[cnt];
    final List<PutFlagSet> flagSets =
        IntStream.range(0, cnt)
            .boxed()
            .map(
                i ->
                    PutFlagSet.of(
                        PutFlags.MDB_APPEND, PutFlags.MDB_NOOVERWRITE, PutFlags.MDB_RESERVE))
            .collect(Collectors.toList());

    Instant time;
    for (int i = 0; i < 5; i++) {
      time = Instant.now();
      for (int j = 0; j < flagSets.size(); j++) {
        PutFlagSet flagSet = flagSets.get(j);
        if (!flagSet.isSet(PutFlags.MDB_RESERVE)) {
          throw new RuntimeException("Not set");
        }
        arr[j] = flagSet.getMask();
      }
      System.out.println("Check: " + Duration.between(time, Instant.now()));

      time = Instant.now();
      for (int j = 0; j < flagSets.size(); j++) {
        PutFlagSet flagSet = flagSets.get(j);
        final int mask = flagSet.getMaskWith(PutFlags.MDB_RESERVE);
        arr[j] = mask;
      }
      System.out.println("Append:" + Duration.between(time, Instant.now()));
    }
  }
}
