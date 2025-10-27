package org.lmdbjava;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

public interface EnvFlagSet extends FlagSet<EnvFlags> {

  static EnvFlagSet empty() {
    return EnvFlagSetImpl.EMPTY;
  }

  static EnvFlagSet of(final EnvFlags envFlag) {
    Objects.requireNonNull(envFlag);
    return envFlag;
  }

  static EnvFlagSet of(final EnvFlags... EnvFlags) {
    return builder()
        .withFlags(EnvFlags)
        .build();
  }

  static EnvFlagSet of(final Collection<EnvFlags> EnvFlags) {
    return builder()
        .withFlags(EnvFlags)
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
