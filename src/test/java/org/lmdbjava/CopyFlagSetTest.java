package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

public class CopyFlagSetTest {

    @Test
    public void testEmpty() {
        final CopyFlagSet copyFlagSet = CopyFlagSet.empty();
        assertThat(
                copyFlagSet.getMask(),
                is(0));
        assertThat(
                copyFlagSet.size(),
                is(0));
        assertThat(
                copyFlagSet.isEmpty(),
                is(true));
        assertThat(
                copyFlagSet.isSet(CopyFlags.MDB_CP_COMPACT),
                is(false));
        final CopyFlagSet copyFlagSet2 = CopyFlagSet.builder()
            .build();
        assertThat(copyFlagSet, is(copyFlagSet2));
        assertThat(copyFlagSet, not(CopyFlagSet.of(CopyFlags.MDB_CP_COMPACT)));
        assertThat(copyFlagSet, not(CopyFlagSet.builder()
            .setFlag(CopyFlags.MDB_CP_COMPACT)
            .build()));
    }

    @Test
    public void testOf() {
        final CopyFlags copyFlag = CopyFlags.MDB_CP_COMPACT;
        final CopyFlagSet copyFlagSet = CopyFlagSet.of(copyFlag);
        assertThat(
                copyFlagSet.getMask(),
                is(MaskedFlag.mask(copyFlag)));
        assertThat(
                copyFlagSet.size(),
                is(1));
        for (CopyFlags flag : copyFlagSet) {
            assertThat(
                    copyFlagSet.isSet(flag),
                    is(true));
        }

        final CopyFlagSet copyFlagSet2 = CopyFlagSet.builder()
            .setFlag(copyFlag)
            .build();
        assertThat(copyFlagSet, is(copyFlagSet2));
    }

    @Test
    public void testBuilder() {
        final CopyFlags copyFlag1 = CopyFlags.MDB_CP_COMPACT;
        final CopyFlagSet copyFlagSet = CopyFlagSet.builder()
                .setFlag(copyFlag1)
                .build();
        assertThat(
                copyFlagSet.getMask(),
                is(MaskedFlag.mask(copyFlag1)));
        assertThat(
                copyFlagSet.size(),
                is(1));
        assertThat(
                copyFlagSet.isSet(CopyFlags.MDB_CP_COMPACT),
                is(true));
        for (CopyFlags flag : copyFlagSet) {
            assertThat(
                    copyFlagSet.isSet(flag),
                    is(true));
        }
        final CopyFlagSet copyFlagSet2 = CopyFlagSet.builder()
                .withFlags(copyFlag1)
                .build();
        final CopyFlagSet copyFlagSet3 = CopyFlagSet.builder()
                .withFlags(new HashSet<>(Collections.singletonList(copyFlag1)))
                .build();
        assertThat(copyFlagSet, is(copyFlagSet2));
        assertThat(copyFlagSet, is(copyFlagSet3));
    }
}
