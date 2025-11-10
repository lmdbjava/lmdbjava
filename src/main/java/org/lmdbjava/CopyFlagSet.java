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

import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

/** An immutable set of flags for use when performing a {@link Env#copy(Path, CopyFlagSet)}. */
public interface CopyFlagSet extends FlagSet<CopyFlags> {

  /** An immutable empty {@link CopyFlagSet}. */
  CopyFlagSet EMPTY = CopyFlagSetImpl.EMPTY;

  /**
   * Gets the immutable empty {@link CopyFlagSet} instance.
   *
   * @return The immutable empty {@link CopyFlagSet} instance.
   */
  static CopyFlagSet empty() {
    return CopyFlagSetImpl.EMPTY;
  }

  /**
   * Creates an immutable {@link CopyFlagSet} containing copyFlag.
   *
   * @param copyFlag The flag to include in the {@link CopyFlagSet}
   * @return An immutable {@link CopyFlagSet} containing just copyFlag.
   */
  static CopyFlagSet of(final CopyFlags copyFlag) {
    Objects.requireNonNull(copyFlag);
    return copyFlag;
  }

  /**
   * Creates an immutable {@link CopyFlagSet} containing copyFlags.
   *
   * @param copyFlags The flags to include in the {@link CopyFlagSet}.
   * @return An immutable {@link CopyFlagSet} containing copyFlags.
   */
  static CopyFlagSet of(final CopyFlags... copyFlags) {
    return builder().setFlags(copyFlags).build();
  }

  /**
   * Creates an immutable {@link CopyFlagSet} containing copyFlags.
   *
   * @param copyFlags The flags to include in the {@link CopyFlagSet}.
   * @return An immutable {@link CopyFlagSet} containing copyFlags.
   */
  static CopyFlagSet of(final Collection<CopyFlags> copyFlags) {
    return builder().setFlags(copyFlags).build();
  }

  /**
   * Create a builder for building an {@link CopyFlagSet}.
   *
   * @return A builder instance for building an {@link CopyFlagSet}.
   */
  static AbstractFlagSet.Builder<CopyFlags, CopyFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        CopyFlags.class, CopyFlagSetImpl::new, copyFlag -> copyFlag, () -> CopyFlagSetImpl.EMPTY);
  }
}
