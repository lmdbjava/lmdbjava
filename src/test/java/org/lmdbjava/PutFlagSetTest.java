package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

public class PutFlagSetTest {

    @Test
    public void testEmpty() {
        final PutFlagSet putFlagSet = PutFlagSet.empty();
        assertThat(
                putFlagSet.getMask(),
                is(0));
        assertThat(
                putFlagSet.size(),
                is(0));
        assertThat(
                putFlagSet.isEmpty(),
                is(true));
        assertThat(
                putFlagSet.isSet(PutFlags.MDB_MULTIPLE),
                is(false));
    }

    @Test
    public void testOf() {
        final PutFlags putFlag = PutFlags.MDB_APPEND;
        final PutFlagSet putFlagSet = PutFlagSet.of(putFlag);
        assertThat(
                putFlagSet.getMask(),
                is(MaskedFlag.mask(putFlag)));
        assertThat(
                putFlagSet.size(),
                is(1));
        assertThat(
                putFlagSet.isSet(PutFlags.MDB_MULTIPLE),
                is(false));
        for (PutFlags flag : putFlagSet) {
            assertThat(
                    putFlagSet.isSet(flag),
                    is(true));
        }
    }

    @Test
    public void testOf2() {
        final PutFlags putFlag1 = PutFlags.MDB_APPEND;
        final PutFlags putFlag2 = PutFlags.MDB_NOOVERWRITE;
        final PutFlagSet putFlagSet = PutFlagSet.of(putFlag1, putFlag2);
        assertThat(
                putFlagSet.getMask(),
                is(MaskedFlag.mask(putFlag1, putFlag2)));
        assertThat(
                putFlagSet.size(),
                is(2));
        assertThat(
                putFlagSet.isSet(PutFlags.MDB_MULTIPLE),
                is(false));
        for (PutFlags flag : putFlagSet) {
            assertThat(
                    putFlagSet.isSet(flag),
                    is(true));
        }
    }

    @Test
    public void testBuilder() {
        final PutFlags putFlag1 = PutFlags.MDB_APPEND;
        final PutFlags putFlag2 = PutFlags.MDB_NOOVERWRITE;
        final PutFlagSet putFlagSet = PutFlagSet.builder()
                .setFlag(putFlag1)
                .setFlag(putFlag2)
                .build();
        assertThat(
                putFlagSet.getMask(),
                is(MaskedFlag.mask(putFlag1, putFlag2)));
        assertThat(
                putFlagSet.size(),
                is(2));
        assertThat(
                putFlagSet.isSet(PutFlags.MDB_MULTIPLE),
                is(false));
        for (PutFlags flag : putFlagSet) {
            assertThat(
                    putFlagSet.isSet(flag),
                    is(true));
        }
        final PutFlagSet putFlagSet2 = PutFlagSet.builder()
                .withFlags(putFlag1, putFlag2)
                .build();
        final PutFlagSet putFlagSet3 = PutFlagSet.builder()
                .withFlags(new HashSet<>(Arrays.asList(putFlag1, putFlag2)))
                .build();
        assertThat(putFlagSet, is(putFlagSet2));
        assertThat(putFlagSet, is(putFlagSet3));
    }
}
