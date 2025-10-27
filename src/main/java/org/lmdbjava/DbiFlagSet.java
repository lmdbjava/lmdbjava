package org.lmdbjava;

import java.util.EnumSet;
import java.util.Objects;

public class DbiFlagSet extends AbstractFlagSet<DbiFlags> {

    public static final DbiFlagSet EMPTY = new DbiFlagSet(EnumSet.noneOf(DbiFlags.class));

    private DbiFlagSet(final EnumSet<DbiFlags> flags) {
        super(flags);
    }

    public static DbiFlagSet empty() {
        return EMPTY;
    }

    public static DbiFlagSet of(final DbiFlags putFlag) {
        Objects.requireNonNull(putFlag);
        return new DbiFlagSet(EnumSet.of(putFlag));
    }

    public static DbiFlagSet of(final DbiFlags... DbiFlags) {
        return builder()
                .withFlags(DbiFlags)
                .build();
    }

    public static Builder<DbiFlags, DbiFlagSet> builder() {
        return new Builder<>(DbiFlags.class, DbiFlagSet::new);
    }
}
