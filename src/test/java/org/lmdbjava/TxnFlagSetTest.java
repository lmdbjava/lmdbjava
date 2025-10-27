package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

public class TxnFlagSetTest {

    @Test
    public void testEmpty() {
        final TxnFlagSet txnFlagSet = TxnFlagSet.empty();
        assertThat(
                txnFlagSet.getMask(),
                is(0));
        assertThat(
                txnFlagSet.size(),
                is(0));
        assertThat(
                txnFlagSet.isEmpty(),
                is(true));
        assertThat(
                txnFlagSet.isSet(TxnFlags.MDB_RDONLY_TXN),
                is(false));
        final TxnFlagSet txnFlagSet2 = TxnFlagSet.builder()
            .build();
        assertThat(txnFlagSet, is(txnFlagSet2));
        assertThat(txnFlagSet, not(TxnFlagSet.of(TxnFlags.MDB_RDONLY_TXN)));
        assertThat(txnFlagSet, not(TxnFlagSet.builder()
            .setFlag(TxnFlags.MDB_RDONLY_TXN)
            .build()));
    }

    @Test
    public void testOf() {
        final TxnFlags txnFlag = TxnFlags.MDB_RDONLY_TXN;
        final TxnFlagSet txnFlagSet = TxnFlagSet.of(txnFlag);
        assertThat(
                txnFlagSet.getMask(),
                is(MaskedFlag.mask(txnFlag)));
        assertThat(
                txnFlagSet.size(),
                is(1));
        for (TxnFlags flag : txnFlagSet) {
            assertThat(
                    txnFlagSet.isSet(flag),
                    is(true));
        }

        final TxnFlagSet txnFlagSet2 = TxnFlagSet.builder()
            .setFlag(txnFlag)
            .build();
        assertThat(txnFlagSet, is(txnFlagSet2));
    }

    @Test
    public void testBuilder() {
        final TxnFlags txnFlag1 = TxnFlags.MDB_RDONLY_TXN;
        final TxnFlagSet txnFlagSet = TxnFlagSet.builder()
                .setFlag(txnFlag1)
                .build();
        assertThat(
                txnFlagSet.getMask(),
                is(MaskedFlag.mask(txnFlag1)));
        assertThat(
                txnFlagSet.size(),
                is(1));
        assertThat(
                txnFlagSet.isSet(TxnFlags.MDB_RDONLY_TXN),
                is(true));
        for (TxnFlags flag : txnFlagSet) {
            assertThat(
                    txnFlagSet.isSet(flag),
                    is(true));
        }
        final TxnFlagSet txnFlagSet2 = TxnFlagSet.builder()
                .withFlags(txnFlag1)
                .build();
        final TxnFlagSet txnFlagSet3 = TxnFlagSet.builder()
                .withFlags(new HashSet<>(Collections.singletonList(txnFlag1)))
                .build();
        assertThat(txnFlagSet, is(txnFlagSet2));
        assertThat(txnFlagSet, is(txnFlagSet3));
    }
}
