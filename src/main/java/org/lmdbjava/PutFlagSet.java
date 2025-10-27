package org.lmdbjava;

import java.util.EnumSet;
import java.util.Objects;

public class PutFlagSet extends FlagSet<PutFlags> {

    public static final PutFlagSet EMPTY = new PutFlagSet(EnumSet.noneOf(PutFlags.class));

    private PutFlagSet(final EnumSet<PutFlags> flags) {
        super(flags);
    }

    public static PutFlagSet empty() {
        return EMPTY;
    }

    public static PutFlagSet of(final PutFlags putFlag) {
        Objects.requireNonNull(putFlag);
        return new org.lmdbjava.PutFlagSet(EnumSet.of(putFlag));
    }

    public static PutFlagSet of(final PutFlags... putFlags) {
        return builder()
                .withFlags(putFlags)
                .build();
    }

    public static Builder<PutFlags, PutFlagSet> builder() {
        return new Builder<>(PutFlags.class, PutFlagSet::new);
    }
}
