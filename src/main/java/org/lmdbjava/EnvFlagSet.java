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

public interface EnvFlagSet extends FlagSet<EnvFlags> {

  EnvFlagSet EMPTY = EnvFlagSetImpl.EMPTY;

  static EnvFlagSet empty() {
    return EnvFlagSetImpl.EMPTY;
  }

  static EnvFlagSet of(final EnvFlags envFlag) {
    Objects.requireNonNull(envFlag);
    return envFlag;
  }

  static EnvFlagSet of(final EnvFlags... EnvFlags) {
    return builder()
        .setFlags(EnvFlags)
        .build();
  }

  static EnvFlagSet of(final Collection<EnvFlags> EnvFlags) {
    return builder()
        .setFlags(EnvFlags)
        .build();
  }

  static AbstractFlagSet.Builder<EnvFlags, EnvFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        EnvFlags.class,
        EnvFlagSetImpl::new,
        envFlag -> envFlag,
        () -> EnvFlagSetImpl.EMPTY);
  }


  // --------------------------------------------------------------------------------


  class EnvFlagSetImpl extends AbstractFlagSet<EnvFlags> implements EnvFlagSet {

    static final EnvFlagSet EMPTY = new EmptyEnvFlagSet();

    private EnvFlagSetImpl(final EnumSet<EnvFlags> flags) {
      super(flags);
    }
  }


  // --------------------------------------------------------------------------------


  class EmptyEnvFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<EnvFlags> implements EnvFlagSet {
  }
}
