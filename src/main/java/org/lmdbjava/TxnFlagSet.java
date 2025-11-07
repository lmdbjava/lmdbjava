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

/** An immutable set of flags for use when creating a {@link Txn}. */
public interface TxnFlagSet extends FlagSet<TxnFlags> {

  /**
   * An immutable empty {@link TxnFlagSet}.
   */
  TxnFlagSet EMPTY = TxnFlagSetImpl.EMPTY;

  /**
   * Gets the immutable empty {@link TxnFlagSet} instance.
   * @return The immutable empty {@link TxnFlagSet} instance.
   */
  static TxnFlagSet empty() {
    return TxnFlagSetImpl.EMPTY;
  }

  /**
   * Creates an immutable {@link TxnFlagSet} containing txnFlag.
   * @param txnFlag The flag to include in the {@link TxnFlagSet}
   * @return An immutable {@link TxnFlagSet} containing just txnFlag.
   */
  static TxnFlagSet of(final TxnFlags txnFlag) {
    Objects.requireNonNull(txnFlag);
    return txnFlag;
  }

  /**
   * Creates an immutable {@link TxnFlagSet} containing txnFlags.
   * @param txnFlags The flags to include in the {@link TxnFlagSet}.
   * @return An immutable {@link TxnFlagSet} containing txnFlags.
   */
  static TxnFlagSet of(final TxnFlags... txnFlags) {
    return builder().setFlags(txnFlags).build();
  }

  /**
   * Creates an immutable {@link TxnFlagSet} containing txnFlags.
   * @param txnFlags The flags to include in the {@link TxnFlagSet}.
   * @return An immutable {@link TxnFlagSet} containing txnFlags.
   */
  static TxnFlagSet of(final Collection<TxnFlags> txnFlags) {
    return builder().setFlags(txnFlags).build();
  }

  /**
   * Create a builder for building an {@link TxnFlagSet}.
   * @return A builder instance for building an {@link TxnFlagSet}.
   */
  static AbstractFlagSet.Builder<TxnFlags, TxnFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        TxnFlags.class, TxnFlagSetImpl::new, txnFlag -> txnFlag, () -> TxnFlagSetImpl.EMPTY);
  }
}
