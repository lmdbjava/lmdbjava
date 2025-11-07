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
import java.util.Objects;

/** An immutable set of flags for use when opening a {@link Dbi}. */
public interface DbiFlagSet extends FlagSet<DbiFlags> {

  /** An immutable empty {@link DbiFlagSet}. */
  DbiFlagSet EMPTY = DbiFlagSetImpl.EMPTY;

  /** The set of {@link DbiFlags} that indicate unsigned integer keys are being used. */
  DbiFlagSet INTEGER_KEY_FLAGS = DbiFlagSet.of(DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_INTEGERDUP);

  /**
   * Gets the immutable empty {@link DbiFlagSet} instance.
   *
   * @return The immutable empty {@link DbiFlagSet} instance.
   */
  static DbiFlagSet empty() {
    return DbiFlagSetImpl.EMPTY;
  }

  /**
   * Creates an immutable {@link DbiFlagSet} containing dbiFlag.
   *
   * @param dbiFlag The flag to include in the {@link DbiFlagSet}
   * @return An immutable {@link DbiFlagSet} containing just dbiFlag.
   */
  static DbiFlagSet of(final DbiFlags dbiFlag) {
    Objects.requireNonNull(dbiFlag);
    return dbiFlag;
  }

  /**
   * Creates an immutable {@link DbiFlagSet} containing dbiFlags.
   *
   * @param dbiFlags The flags to include in the {@link DbiFlagSet}.
   * @return An immutable {@link DbiFlagSet} containing dbiFlags.
   */
  static DbiFlagSet of(final DbiFlags... dbiFlags) {
    return builder().setFlags(dbiFlags).build();
  }

  /**
   * Creates an immutable {@link DbiFlagSet} containing dbiFlags.
   *
   * @param dbiFlags The flags to include in the {@link DbiFlagSet}.
   * @return An immutable {@link DbiFlagSet} containing dbiFlags.
   */
  static DbiFlagSet of(final Collection<DbiFlags> dbiFlags) {
    return builder().setFlags(dbiFlags).build();
  }

  /**
   * Create a builder for building an {@link DbiFlagSet}.
   *
   * @return A builder instance for building an {@link DbiFlagSet}.
   */
  static AbstractFlagSet.Builder<DbiFlags, DbiFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        DbiFlags.class, DbiFlagSetImpl::new, dbiFlag -> dbiFlag, () -> DbiFlagSetImpl.EMPTY);
  }
}
