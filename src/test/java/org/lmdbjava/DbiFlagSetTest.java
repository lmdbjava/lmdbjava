package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

public class DbiFlagSetTest {

    @Test
    public void testEmpty() {
        final DbiFlagSet putFlagSet = DbiFlagSet.empty();
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
                putFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
    }

    @Test
    public void testOf() {
        final DbiFlags putFlag = DbiFlags.MDB_CREATE;
        final DbiFlagSet putFlagSet = DbiFlagSet.of(putFlag);
        assertThat(
                putFlagSet.getMask(),
                is(MaskedFlag.mask(putFlag)));
        assertThat(
                putFlagSet.size(),
                is(1));
        assertThat(
                putFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        for (DbiFlags flag : putFlagSet) {
            assertThat(
                    putFlagSet.isSet(flag),
                    is(true));
        }
    }

    @Test
    public void testOf2() {
        final DbiFlags putFlag1 = DbiFlags.MDB_CREATE;
        final DbiFlags putFlag2 = DbiFlags.MDB_INTEGERKEY;
        final DbiFlagSet putFlagSet = DbiFlagSet.of(putFlag1, putFlag2);
        assertThat(
                putFlagSet.getMask(),
                is(MaskedFlag.mask(putFlag1, putFlag2)));
        assertThat(
                putFlagSet.size(),
                is(2));
        assertThat(
                putFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        for (DbiFlags flag : putFlagSet) {
            assertThat(
                    putFlagSet.isSet(flag),
                    is(true));
        }
    }

    @Test
    public void testBuilder() {
        final DbiFlags putFlag1 = DbiFlags.MDB_CREATE;
        final DbiFlags putFlag2 = DbiFlags.MDB_INTEGERKEY;
        final DbiFlagSet putFlagSet = DbiFlagSet.builder()
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
                putFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        for (DbiFlags flag : putFlagSet) {
            assertThat(
                    putFlagSet.isSet(flag),
                    is(true));
        }
        final DbiFlagSet putFlagSet2 = DbiFlagSet.builder()
                .withFlags(putFlag1, putFlag2)
                .build();
        final DbiFlagSet putFlagSet3 = DbiFlagSet.builder()
                .withFlags(new HashSet<>(Arrays.asList(putFlag1, putFlag2)))
                .build();
        assertThat(putFlagSet, is(putFlagSet2));
        assertThat(putFlagSet, is(putFlagSet3));
    }
}
