package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

public class EnvFlagSetTest {

    @Test
    public void testEmpty() {
        final EnvFlagSet envFlagSet = EnvFlagSet.empty();
        assertThat(
                envFlagSet.getMask(),
                is(0));
        assertThat(
                envFlagSet.size(),
                is(0));
        assertThat(
                envFlagSet.isEmpty(),
                is(true));
        assertThat(
                envFlagSet.isSet(EnvFlags.MDB_NOSUBDIR),
                is(false));
        final EnvFlagSet envFlagSet2 = EnvFlagSet.builder()
            .build();
        assertThat(envFlagSet, is(envFlagSet2));
        assertThat(envFlagSet, not(EnvFlagSet.of(EnvFlags.MDB_FIXEDMAP)));
        assertThat(envFlagSet, not(EnvFlagSet.of(EnvFlags.MDB_FIXEDMAP, EnvFlags.MDB_NORDAHEAD)));
        assertThat(envFlagSet, not(EnvFlagSet.builder()
            .setFlag(EnvFlags.MDB_FIXEDMAP)
            .setFlag(EnvFlags.MDB_NORDAHEAD)
            .build()));
    }

    @Test
    public void testOf() {
        final EnvFlags envFlag = EnvFlags.MDB_FIXEDMAP;
        final EnvFlagSet envFlagSet = EnvFlagSet.of(envFlag);
        assertThat(
                envFlagSet.getMask(),
                is(MaskedFlag.mask(envFlag)));
        assertThat(
                envFlagSet.size(),
                is(1));
        assertThat(
                envFlagSet.isSet(EnvFlags.MDB_NOSUBDIR),
                is(false));
        for (EnvFlags flag : envFlagSet) {
            assertThat(
                    envFlagSet.isSet(flag),
                    is(true));
        }

        final EnvFlagSet envFlagSet2 = EnvFlagSet.builder()
            .setFlag(envFlag)
            .build();
        assertThat(envFlagSet, is(envFlagSet2));
    }

    @Test
    public void testOf2() {
        final EnvFlags envFlag1 = EnvFlags.MDB_FIXEDMAP;
        final EnvFlags envFlag2 = EnvFlags.MDB_NORDAHEAD;
        final EnvFlagSet envFlagSet = EnvFlagSet.of(envFlag1, envFlag2);
        assertThat(
                envFlagSet.getMask(),
                is(MaskedFlag.mask(envFlag1, envFlag2)));
        assertThat(
                envFlagSet.size(),
                is(2));
        assertThat(
                envFlagSet.isSet(EnvFlags.MDB_WRITEMAP),
                is(false));
        for (EnvFlags flag : envFlagSet) {
            assertThat(
                    envFlagSet.isSet(flag),
                    is(true));
        }
    }

    @Test
    public void testBuilder() {
        final EnvFlags envFlag1 = EnvFlags.MDB_FIXEDMAP;
        final EnvFlags envFlag2 = EnvFlags.MDB_NORDAHEAD;
        final EnvFlagSet envFlagSet = EnvFlagSet.builder()
                .setFlag(envFlag1)
                .setFlag(envFlag2)
                .build();
        assertThat(
                envFlagSet.getMask(),
                is(MaskedFlag.mask(envFlag1, envFlag2)));
        assertThat(
                envFlagSet.size(),
                is(2));
        assertThat(
                envFlagSet.isSet(EnvFlags.MDB_NOTLS),
                is(false));
        for (EnvFlags flag : envFlagSet) {
            assertThat(
                    envFlagSet.isSet(flag),
                    is(true));
        }
        final EnvFlagSet envFlagSet2 = EnvFlagSet.builder()
                .withFlags(envFlag1, envFlag2)
                .build();
        final EnvFlagSet envFlagSet3 = EnvFlagSet.builder()
                .withFlags(new HashSet<>(Arrays.asList(envFlag1, envFlag2)))
                .build();
        assertThat(envFlagSet, is(envFlagSet2));
        assertThat(envFlagSet, is(envFlagSet3));
    }
}
