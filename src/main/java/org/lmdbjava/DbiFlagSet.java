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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

public interface DbiFlagSet extends FlagSet<DbiFlags> {

  /** An immutable empty {@link DbiFlagSet}. */
  DbiFlagSet EMPTY = DbiFlagSetImpl.EMPTY;

  /** The set of {@link DbiFlags} that indicate unsigned integer keys are being used. */
  DbiFlagSet INTEGER_KEY_FLAGS = DbiFlagSet.of(
      DbiFlags.MDB_INTEGERKEY,
      DbiFlags.MDB_INTEGERDUP);

  static DbiFlagSet empty() {
    return DbiFlagSetImpl.EMPTY;
  }

  static DbiFlagSet of(final DbiFlags dbiFlag) {
    Objects.requireNonNull(dbiFlag);
    return dbiFlag;
  }

  static DbiFlagSet of(final DbiFlags... DbiFlags) {
    return builder()
        .withFlags(DbiFlags)
        .build();
  }

  static DbiFlagSet of(final Collection<DbiFlags> DbiFlags) {
    return builder()
        .withFlags(DbiFlags)
        .build();
  }

  static AbstractFlagSet.Builder<DbiFlags, DbiFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        DbiFlags.class,
        DbiFlagSetImpl::new,
        dbiFlag -> dbiFlag,
        () -> DbiFlagSetImpl.EMPTY);
  }


  // --------------------------------------------------------------------------------


  class DbiFlagSetImpl extends AbstractFlagSet<DbiFlags> implements DbiFlagSet {

    static final DbiFlagSet EMPTY = new EmptyDbiFlagSet();

    private DbiFlagSetImpl(final EnumSet<DbiFlags> flags) {
      super(flags);
    }
  }


  // --------------------------------------------------------------------------------


  class EmptyDbiFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<DbiFlags> implements DbiFlagSet {
  }
}
