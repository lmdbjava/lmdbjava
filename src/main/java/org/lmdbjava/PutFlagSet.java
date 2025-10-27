package org.lmdbjava;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

public interface PutFlagSet extends FlagSet<PutFlags> {

    static PutFlagSet empty() {
        return PutFlagSetImpl.EMPTY;
    }

    static PutFlagSet of(final PutFlags putFlag) {
        Objects.requireNonNull(putFlag);
        return putFlag;
    }

    static PutFlagSet of(final PutFlags... putFlags) {
        return builder()
                .withFlags(putFlags)
                .build();
    }

    static PutFlagSet of(final Collection<PutFlags> putFlags) {
        return builder()
            .withFlags(putFlags)
            .build();
    }

    static AbstractFlagSet.Builder<PutFlags, PutFlagSet> builder() {
        return new AbstractFlagSet.Builder<>(
            PutFlags.class,
            PutFlagSetImpl::new,
            putFlag -> putFlag,
            EmptyPutFlagSet::new);
    }


    // --------------------------------------------------------------------------------


    class PutFlagSetImpl extends AbstractFlagSet<PutFlags> implements PutFlagSet {

        public static final PutFlagSet EMPTY = new EmptyPutFlagSet();

        private PutFlagSetImpl(final EnumSet<PutFlags> flags) {
            super(flags);
        }
    }


    // --------------------------------------------------------------------------------


    class EmptyPutFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<PutFlags> implements PutFlagSet {
    }
}
