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

class DbiFlagSetTest extends AbstractFlagSetTest<DbiFlags, DbiFlagSet> {

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
}
