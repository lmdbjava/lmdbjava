/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

public class TxnFlagSetTest {

    @Test
    public void testEmpty() {
        final TxnFlagSet txnFlagSet = TxnFlagSet.empty();
        assertThat(txnFlagSet.getMask()).isEqualTo(0);
        assertThat(txnFlagSet.size()).isEqualTo(0);
        assertThat(txnFlagSet.isEmpty()).isEqualTo(true);
        assertThat(txnFlagSet.isSet(TxnFlags.MDB_RDONLY_TXN)).isEqualTo(false);
        final TxnFlagSet txnFlagSet2 = TxnFlagSet.builder()
            .build();
        assertThat(txnFlagSet).isEqualTo(txnFlagSet2);
        assertThat(txnFlagSet).isNotEqualTo(TxnFlagSet.of(TxnFlags.MDB_RDONLY_TXN));
        assertThat(txnFlagSet).isNotEqualTo(TxnFlagSet.builder()
            .setFlag(TxnFlags.MDB_RDONLY_TXN)
            .build());
    }

    @Test
    public void testOf() {
        final TxnFlags txnFlag = TxnFlags.MDB_RDONLY_TXN;
        final TxnFlagSet txnFlagSet = TxnFlagSet.of(txnFlag);
        assertThat(txnFlagSet.getMask()).isEqualTo(MaskedFlag.mask(txnFlag));
        assertThat(txnFlagSet.size()).isEqualTo(1);
        for (TxnFlags flag : txnFlagSet) {
            assertThat(txnFlagSet.isSet(flag)).isEqualTo(true);
        }

        final TxnFlagSet txnFlagSet2 = TxnFlagSet.builder()
            .setFlag(txnFlag)
            .build();
        assertThat(txnFlagSet).isEqualTo(txnFlagSet2);
    }

    @Test
    public void testBuilder() {
        final TxnFlags txnFlag1 = TxnFlags.MDB_RDONLY_TXN;
        final TxnFlagSet txnFlagSet = TxnFlagSet.builder()
                .setFlag(txnFlag1)
                .build();
        assertThat(txnFlagSet.getMask()).isEqualTo(MaskedFlag.mask(txnFlag1));
        assertThat(txnFlagSet.size()).isEqualTo(1);
        assertThat(txnFlagSet.isSet(TxnFlags.MDB_RDONLY_TXN)).isEqualTo(true);
        for (TxnFlags flag : txnFlagSet) {
            assertThat(txnFlagSet.isSet(flag)).isEqualTo(true);
        }
        final TxnFlagSet txnFlagSet2 = TxnFlagSet.builder()
                .withFlags(txnFlag1)
                .build();
        final TxnFlagSet txnFlagSet3 = TxnFlagSet.builder()
                .withFlags(new HashSet<>(Collections.singletonList(txnFlag1)))
                .build();
        assertThat(txnFlagSet).isEqualTo(txnFlagSet2);
        assertThat(txnFlagSet).isEqualTo(txnFlagSet3);
    }
}
