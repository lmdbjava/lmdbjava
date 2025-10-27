package org.lmdbjava;

import java.util.EnumSet;
import java.util.Objects;

public interface TxnFlagSet extends FlagSet<TxnFlags> {

  TxnFlagSet EMPTY = TxnFlagSetImpl.EMPTY;

  static TxnFlagSet empty() {
    return TxnFlagSetImpl.EMPTY;
  }

  static TxnFlagSet of(final TxnFlags putFlag) {
    Objects.requireNonNull(putFlag);
    return new SingleTxnFlagSet(putFlag);
  }

  static TxnFlagSet of(final TxnFlags... TxnFlags) {
    return builder()
        .withFlags(TxnFlags)
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
