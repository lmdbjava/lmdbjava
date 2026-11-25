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

/** An immutable set of flags for use when performing a "put". */
public interface PutFlagSet extends FlagSet<PutFlags> {

  /** An immutable empty {@link PutFlagSet}. */
  PutFlagSet EMPTY = PutFlagSetImpl.EMPTY;

  /**
   * Gets the immutable empty {@link PutFlagSet} instance.
   *
   * @return The immutable empty {@link PutFlagSet} instance.
   */
  static PutFlagSet empty() {
    return PutFlagSetImpl.EMPTY;
  }

  /**
   * Creates an immutable {@link PutFlagSet} containing putFlag.
   *
   * @param putFlag The flag to include in the {@link PutFlagSet}
   * @return An immutable {@link PutFlagSet} containing just putFlag.
   */
  static PutFlagSet of(final PutFlags putFlag) {
    Objects.requireNonNull(putFlag);
    return putFlag;
  }

  /**
   * Creates an immutable {@link PutFlagSet} containing putFlags.
   *
   * @param putFlags The flags to include in the {@link PutFlagSet}.
   * @return An immutable {@link PutFlagSet} containing putFlags.
   */
  static PutFlagSet of(final PutFlags... putFlags) {
    return builder().setFlags(putFlags).build();
  }

  /**
   * Creates an immutable {@link PutFlagSet} containing putFlags.
   *
   * @param putFlags The flags to include in the {@link PutFlagSet}.
   * @return An immutable {@link PutFlagSet} containing putFlags.
   */
  static PutFlagSet of(final Collection<PutFlags> putFlags) {
    return builder().setFlags(putFlags).build();
  }

  /**
   * Create a builder for building an {@link PutFlagSet}.
   *
   * @return A builder instance for building an {@link PutFlagSet}.
   */
  static AbstractFlagSet.Builder<PutFlags, PutFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        PutFlags.class, PutFlagSetImpl::new, putFlag -> putFlag, PutFlagSetEmpty::new);
  }
}
