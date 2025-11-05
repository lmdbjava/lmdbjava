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

import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

public class DbiFlagSetTest {

  @Test
  public void testEmpty() {
    final DbiFlagSet dbiFlagSet = DbiFlagSet.empty();
    assertThat(dbiFlagSet.getMask()).isEqualTo(0);
    assertThat(dbiFlagSet.size()).isEqualTo(0);
    assertThat(dbiFlagSet.isEmpty()).isEqualTo(true);
    assertThat(dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP)).isEqualTo(false);
    final DbiFlagSet dbiFlagSet2 = DbiFlagSet.builder().build();
    assertThat(dbiFlagSet).isEqualTo(dbiFlagSet2);
    assertThat(dbiFlagSet).isNotEqualTo(DbiFlagSet.of(DbiFlags.MDB_CREATE));
    assertThat(dbiFlagSet).isNotEqualTo(DbiFlagSet.of(DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT));
    assertThat(dbiFlagSet)
        .isNotEqualTo(
            DbiFlagSet.builder()
                .setFlag(DbiFlags.MDB_CREATE)
                .setFlag(DbiFlags.MDB_DUPFIXED)
                .build());
  }

  @Test
  public void testOf() {
    final DbiFlags dbiFlag = DbiFlags.MDB_CREATE;
    final DbiFlagSet dbiFlagSet = DbiFlagSet.of(dbiFlag);
    assertThat(dbiFlagSet.getMask()).isEqualTo(MaskedFlag.mask(dbiFlag));
    assertThat(dbiFlagSet.size()).isEqualTo(1);
    assertThat(dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP)).isEqualTo(false);
    for (DbiFlags flag : dbiFlagSet) {
      assertThat(dbiFlagSet.isSet(flag)).isEqualTo(true);
    }

    final DbiFlagSet dbiFlagSet2 = DbiFlagSet.builder().setFlag(dbiFlag).build();
    assertThat(dbiFlagSet).isEqualTo(dbiFlagSet2);
  }

  @Test
  public void testOf2() {
    final DbiFlags dbiFlag1 = DbiFlags.MDB_CREATE;
    final DbiFlags dbiFlag2 = DbiFlags.MDB_INTEGERKEY;
    final DbiFlagSet dbiFlagSet = DbiFlagSet.of(dbiFlag1, dbiFlag2);
    assertThat(dbiFlagSet.getMask()).isEqualTo(MaskedFlag.mask(dbiFlag1, dbiFlag2));
    assertThat(dbiFlagSet.size()).isEqualTo(2);
    assertThat(dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP)).isEqualTo(false);
    for (DbiFlags flag : dbiFlagSet) {
      assertThat(dbiFlagSet.isSet(flag)).isEqualTo(true);
    }
  }

  @Test
  public void testBuilder() {
    final DbiFlags dbiFlag1 = DbiFlags.MDB_CREATE;
    final DbiFlags dbiFlag2 = DbiFlags.MDB_INTEGERKEY;
    final DbiFlagSet dbiFlagSet = DbiFlagSet.builder().setFlag(dbiFlag1).setFlag(dbiFlag2).build();
    assertThat(dbiFlagSet.getMask()).isEqualTo(MaskedFlag.mask(dbiFlag1, dbiFlag2));
    assertThat(dbiFlagSet.size()).isEqualTo(2);
    assertThat(dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP)).isEqualTo(false);
    for (DbiFlags flag : dbiFlagSet) {
      assertThat(dbiFlagSet.isSet(flag)).isEqualTo(true);
    }
    final DbiFlagSet dbiFlagSet2 = DbiFlagSet.builder().withFlags(dbiFlag1, dbiFlag2).build();
    final DbiFlagSet dbiFlagSet3 =
        DbiFlagSet.builder().withFlags(new HashSet<>(Arrays.asList(dbiFlag1, dbiFlag2))).build();
    assertThat(dbiFlagSet).isEqualTo(dbiFlagSet2);
    assertThat(dbiFlagSet).isEqualTo(dbiFlagSet3);
  }
}
