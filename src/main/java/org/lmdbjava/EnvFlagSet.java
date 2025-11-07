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

/** An immutable set of flags for use when opening the {@link Env}. */
public interface EnvFlagSet extends FlagSet<EnvFlags> {

  /**
   * An immutable empty {@link EnvFlagSet}.
   */
  EnvFlagSet EMPTY = EnvFlagSetImpl.EMPTY;

  /**
   * Gets the immutable empty {@link EnvFlagSet} instance.
   * @return The immutable empty {@link EnvFlagSet} instance.
   */
  static EnvFlagSet empty() {
    return EnvFlagSetImpl.EMPTY;
  }

  /**
   * Creates an immutable {@link EnvFlagSet} containing envFlag.
   * @param envFlag The flag to include in the {@link EnvFlagSet}
   * @return An immutable {@link EnvFlagSet} containing just envFlag.
   */
  static EnvFlagSet of(final EnvFlags envFlag) {
    Objects.requireNonNull(envFlag);
    return envFlag;
  }

  /**
   * Creates an immutable {@link EnvFlagSet} containing envFlags.
   * @param envFlags The flags to include in the {@link EnvFlagSet}.
   * @return An immutable {@link EnvFlagSet} containing envFlags.
   */
  static EnvFlagSet of(final EnvFlags... envFlags) {
    return builder().setFlags(envFlags).build();
  }

  /**
   * Creates an immutable {@link EnvFlagSet} containing envFlags.
   * @param envFlags The flags to include in the {@link EnvFlagSet}.
   * @return An immutable {@link EnvFlagSet} containing envFlags.
   */
  static EnvFlagSet of(final Collection<EnvFlags> envFlags) {
    return builder().setFlags(envFlags).build();
  }

  /**
   * Create a builder for building an {@link EnvFlagSet}.
   * @return A builder instance for building an {@link EnvFlagSet}.
   */
  static AbstractFlagSet.Builder<EnvFlags, EnvFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        EnvFlags.class, EnvFlagSetImpl::new, envFlag -> envFlag, () -> EnvFlagSetImpl.EMPTY);
  }
}
