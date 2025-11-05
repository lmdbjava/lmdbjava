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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class PutFlagSetTest {

  @Test
  public void testEmpty() {
    final PutFlagSet putFlagSet = PutFlagSet.empty();
    assertThat(putFlagSet.getMask()).isEqualTo(0);
    assertThat(putFlagSet.size()).isEqualTo(0);
    assertThat(putFlagSet.isEmpty()).isEqualTo(true);
    assertThat(putFlagSet.isSet(PutFlags.MDB_MULTIPLE)).isEqualTo(false);
    final PutFlagSet putFlagSet2 = PutFlagSet.builder().build();
    assertThat(putFlagSet).isEqualTo(putFlagSet2);
    assertThat(putFlagSet).isNotEqualTo(PutFlagSet.of(PutFlags.MDB_APPEND));
    assertThat(putFlagSet).isNotEqualTo(PutFlagSet.of(PutFlags.MDB_APPEND, PutFlags.MDB_RESERVE));
    assertThat(putFlagSet)
        .isNotEqualTo(
            PutFlagSet.builder()
                .setFlag(PutFlags.MDB_CURRENT)
                .setFlag(PutFlags.MDB_MULTIPLE)
                .build());
  }

  @Test
  public void testOf() {
    final PutFlags putFlag = PutFlags.MDB_APPEND;
    final PutFlagSet putFlagSet = PutFlagSet.of(putFlag);
    assertThat(putFlagSet.getMask()).isEqualTo(MaskedFlag.mask(putFlag));
    assertThat(putFlagSet.size()).isEqualTo(1);
    assertThat(putFlagSet.isSet(PutFlags.MDB_MULTIPLE)).isEqualTo(false);
    for (PutFlags flag : putFlagSet) {
      assertThat(putFlagSet.isSet(flag)).isEqualTo(true);
    }

    final PutFlagSet putFlagSet2 = PutFlagSet.builder().setFlag(putFlag).build();
    assertThat(putFlagSet).isEqualTo(putFlagSet2);
  }

  @Test
  public void testOf2() {
    final PutFlags putFlag1 = PutFlags.MDB_APPEND;
    final PutFlags putFlag2 = PutFlags.MDB_NOOVERWRITE;
    final PutFlagSet putFlagSet = PutFlagSet.of(putFlag1, putFlag2);
    assertThat(putFlagSet.getMask()).isEqualTo(MaskedFlag.mask(putFlag1, putFlag2));
    assertThat(putFlagSet.size()).isEqualTo(2);
    assertThat(putFlagSet.isSet(PutFlags.MDB_MULTIPLE)).isEqualTo(false);
    for (PutFlags flag : putFlagSet) {
      assertThat(putFlagSet.isSet(flag)).isEqualTo(true);
    }
  }

  @Test
  public void testBuilder() {
    final PutFlags putFlag1 = PutFlags.MDB_APPEND;
    final PutFlags putFlag2 = PutFlags.MDB_NOOVERWRITE;
    final PutFlagSet putFlagSet = PutFlagSet.builder().setFlag(putFlag1).setFlag(putFlag2).build();
    assertThat(putFlagSet.getMask()).isEqualTo(MaskedFlag.mask(putFlag1, putFlag2));
    assertThat(putFlagSet.size()).isEqualTo(2);
    assertThat(putFlagSet.isSet(PutFlags.MDB_MULTIPLE)).isEqualTo(false);
    for (PutFlags flag : putFlagSet) {
      assertThat(putFlagSet.isSet(flag)).isEqualTo(true);
    }
    final PutFlagSet putFlagSet2 = PutFlagSet.builder().withFlags(putFlag1, putFlag2).build();
    final PutFlagSet putFlagSet3 =
        PutFlagSet.builder().withFlags(new HashSet<>(Arrays.asList(putFlag1, putFlag2))).build();
    assertThat(putFlagSet).isEqualTo(putFlagSet2);
    assertThat(putFlagSet).isEqualTo(putFlagSet3);
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
