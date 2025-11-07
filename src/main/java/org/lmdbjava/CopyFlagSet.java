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

public interface CopyFlagSet extends FlagSet<CopyFlags> {

  CopyFlagSet EMPTY = CopyFlagSetImpl.EMPTY;

  static CopyFlagSet empty() {
    return CopyFlagSetImpl.EMPTY;
  }

  static CopyFlagSet of(final CopyFlags dbiFlag) {
    Objects.requireNonNull(dbiFlag);
    return dbiFlag;
  }

  static CopyFlagSet of(final CopyFlags... CopyFlags) {
    return builder().setFlags(CopyFlags).build();
  }

  static CopyFlagSet of(final Collection<CopyFlags> CopyFlags) {
    return builder().setFlags(CopyFlags).build();
  }

  static AbstractFlagSet.Builder<CopyFlags, CopyFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        CopyFlags.class, CopyFlagSetImpl::new, copyFlag -> copyFlag, () -> CopyFlagSetImpl.EMPTY);
  }

  // --------------------------------------------------------------------------------

  class CopyFlagSetImpl extends AbstractFlagSet<CopyFlags> implements CopyFlagSet {

    static final CopyFlagSet EMPTY = new EmptyCopyFlagSet();

    private CopyFlagSetImpl(final EnumSet<CopyFlags> flags) {
      super(flags);
    }
  }

  // --------------------------------------------------------------------------------

  class EmptyCopyFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<CopyFlags>
      implements CopyFlagSet {}
}
