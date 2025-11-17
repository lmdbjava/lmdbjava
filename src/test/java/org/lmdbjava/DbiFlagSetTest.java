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
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DbiFlagSetTest extends AbstractFlagSetTest<DbiFlags, DbiFlagSet> {

  @Test
  void test() {
    // This is here purely to stop CodeQL moaning that this class is unused.
    // All the actual tests are in the superclass
    Assertions.assertThat(getAllFlags()).isNotNull();
  }

  @Override
  List<DbiFlags> getAllFlags() {
    return Arrays.stream(DbiFlags.values()).collect(Collectors.toList());
  }

  @Override
  DbiFlagSet getEmptyFlagSet() {
    return DbiFlagSet.empty();
  }

  @Override
  AbstractFlagSet.Builder<DbiFlags, DbiFlagSet> getBuilder() {
    return DbiFlagSet.builder();
  }

  @Override
  Class<DbiFlags> getFlagType() {
    return DbiFlags.class;
  }

  @Override
  DbiFlagSet getFlagSet(Collection<DbiFlags> flags) {
    return DbiFlagSet.of(flags);
  }

  @Override
  DbiFlagSet getFlagSet(DbiFlags[] flags) {
    return DbiFlagSet.of(flags);
  }

  @Override
  DbiFlagSet getFlagSet(DbiFlags flag) {
    return DbiFlagSet.of(flag);
  }

  @Override
  Function<EnumSet<DbiFlags>, DbiFlagSet> getConstructor() {
    return DbiFlagSetImpl::new;
  }

  /**
   * {@link FlagSet#isSet(MaskedFlag)} on the flag enum is tested in {@link AbstractFlagSetTest} but the coverage check
   * doesn't seem to notice it.
   */
  @Test
  void testIsSet() {
    assertThat(DbiFlags.MDB_CREATE.isSet(DbiFlags.MDB_CREATE))
        .isTrue();
    assertThat(DbiFlags.MDB_CREATE.isSet(DbiFlags.MDB_REVERSEKEY))
        .isFalse();
    //noinspection ConstantValue
    assertThat(DbiFlags.MDB_CREATE.isSet(null))
        .isFalse();
  }
}
