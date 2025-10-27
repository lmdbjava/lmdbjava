package org.lmdbjava;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

public interface CopyFlagSet extends FlagSet<CopyFlags> {

  static CopyFlagSet EMPTY = CopyFlagSetImpl.EMPTY;

  static CopyFlagSet empty() {
    return CopyFlagSetImpl.EMPTY;
  }

  static CopyFlagSet of(final CopyFlags dbiFlag) {
    Objects.requireNonNull(dbiFlag);
    return dbiFlag;
  }

  static CopyFlagSet of(final CopyFlags... CopyFlags) {
    return builder()
        .withFlags(CopyFlags)
        .build();
  }

  static CopyFlagSet of(final Collection<CopyFlags> CopyFlags) {
    return builder()
        .withFlags(CopyFlags)
        .build();
  }

  static AbstractFlagSet.Builder<CopyFlags, CopyFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        CopyFlags.class,
        CopyFlagSetImpl::new,
        copyFlag -> copyFlag,
        () -> CopyFlagSetImpl.EMPTY);
  }


  // --------------------------------------------------------------------------------


  class CopyFlagSetImpl extends AbstractFlagSet<CopyFlags> implements CopyFlagSet {

    static final CopyFlagSet EMPTY = new EmptyCopyFlagSet();

    private CopyFlagSetImpl(final EnumSet<CopyFlags> flags) {
      super(flags);
    }
  }


  // --------------------------------------------------------------------------------


  class EmptyCopyFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<CopyFlags> implements CopyFlagSet {
  }
}
