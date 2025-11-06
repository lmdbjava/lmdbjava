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

public interface TxnFlagSet extends FlagSet<TxnFlags> {

  TxnFlagSet EMPTY = TxnFlagSetImpl.EMPTY;

  static TxnFlagSet empty() {
    return TxnFlagSetImpl.EMPTY;
  }

  static TxnFlagSet of(final TxnFlags putflag) {
    Objects.requireNonNull(putflag);
    return new SingleTxnFlagSet(putflag);
  }

  static TxnFlagSet of(final TxnFlags... TxnFlags) {
    return builder()
        .setFlags(TxnFlags)
        .build();
  }

  static TxnFlagSet of(final Collection<TxnFlags> txnFlags) {
    return builder()
        .setFlags(txnFlags)
        .build();
  }

  static AbstractFlagSet.Builder<TxnFlags, TxnFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        TxnFlags.class,
        TxnFlagSetImpl::new,
        SingleTxnFlagSet::new,
        () -> TxnFlagSetImpl.EMPTY);
  }


  // --------------------------------------------------------------------------------


  class TxnFlagSetImpl extends AbstractFlagSet<TxnFlags> implements TxnFlagSet {

    static final TxnFlagSet EMPTY = new EmptyTxnFlagSet();

    private TxnFlagSetImpl(final EnumSet<TxnFlags> flags) {
      super(flags);
    }
  }


  // --------------------------------------------------------------------------------


  class SingleTxnFlagSet extends AbstractFlagSet.AbstractSingleFlagSet<TxnFlags> implements TxnFlagSet {

    SingleTxnFlagSet(final TxnFlags flag) {
      super(flag);
    }
  }


  // --------------------------------------------------------------------------------


  class EmptyTxnFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<TxnFlags> implements TxnFlagSet {
  }
}
