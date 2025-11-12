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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class EnvFlagSetTest extends AbstractFlagSetTest<EnvFlags, EnvFlagSet> {

  @Test
  void test() {
    // This is here purely to stop CodeQL moaning that this class is unused.
    // All the actual tests are in the superclass
    Assertions.assertThat(getAllFlags())
        .isNotNull();
  }

  @Override
  List<EnvFlags> getAllFlags() {
    return Arrays.stream(EnvFlags.values()).collect(Collectors.toList());
  }

  @Override
  EnvFlagSet getEmptyFlagSet() {
    return EnvFlagSet.empty();
  }

  @Override
  AbstractFlagSet.Builder<EnvFlags, EnvFlagSet> getBuilder() {
    return EnvFlagSet.builder();
  }

  @Override
  EnvFlagSet getFlagSet(Collection<EnvFlags> flags) {
    return EnvFlagSet.of(flags);
  }

  @Override
  EnvFlagSet getFlagSet(EnvFlags[] flags) {
    return EnvFlagSet.of(flags);
  }

  @Override
  EnvFlagSet getFlagSet(EnvFlags flag) {
    return EnvFlagSet.of(flag);
  }

  @Override
  Class<EnvFlags> getFlagType() {
    return EnvFlags.class;
  }
}
