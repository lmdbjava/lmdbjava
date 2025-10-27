package org.lmdbjava;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

public interface DbiFlagSet extends FlagSet<DbiFlags> {

  static DbiFlagSet empty() {
    return DbiFlagSetImpl.EMPTY;
  }

  static DbiFlagSet of(final DbiFlags dbiFlag) {
    Objects.requireNonNull(dbiFlag);
    return dbiFlag;
  }

  static DbiFlagSet of(final DbiFlags... DbiFlags) {
    return builder()
        .withFlags(DbiFlags)
        .build();
  }

  static DbiFlagSet of(final Collection<DbiFlags> DbiFlags) {
    return builder()
        .withFlags(DbiFlags)
        .build();
  }

  static AbstractFlagSet.Builder<DbiFlags, DbiFlagSet> builder() {
    return new AbstractFlagSet.Builder<>(
        DbiFlags.class,
        DbiFlagSetImpl::new,
        dbiFlag -> dbiFlag,
        () -> DbiFlagSetImpl.EMPTY);
  }


  // --------------------------------------------------------------------------------


  class DbiFlagSetImpl extends AbstractFlagSet<DbiFlags> implements DbiFlagSet {

    static final DbiFlagSet EMPTY = new EmptyDbiFlagSet();

    private DbiFlagSetImpl(final EnumSet<DbiFlags> flags) {
      super(flags);
    }
  }


  // --------------------------------------------------------------------------------


  class EmptyDbiFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<DbiFlags> implements DbiFlagSet {
  }
}
