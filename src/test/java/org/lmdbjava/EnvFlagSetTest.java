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

import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

public class EnvFlagSetTest {

    @Test
    public void testEmpty() {
        final EnvFlagSet envFlagSet = EnvFlagSet.empty();
        assertThat(envFlagSet.getMask()).isEqualTo(0);
        assertThat(envFlagSet.size()).isEqualTo(0);
        assertThat(envFlagSet.isEmpty()).isEqualTo(true);
        assertThat(envFlagSet.isSet(EnvFlags.MDB_NOSUBDIR)).isEqualTo(false);
        final EnvFlagSet envFlagSet2 = EnvFlagSet.builder()
            .build();
        assertThat(envFlagSet).isEqualTo(envFlagSet2);
        assertThat(envFlagSet).isNotEqualTo(EnvFlagSet.of(EnvFlags.MDB_FIXEDMAP));
        assertThat(envFlagSet).isNotEqualTo(EnvFlagSet.of(EnvFlags.MDB_FIXEDMAP, EnvFlags.MDB_NORDAHEAD));
        assertThat(envFlagSet).isNotEqualTo(EnvFlagSet.builder()
            .setFlag(EnvFlags.MDB_FIXEDMAP)
            .setFlag(EnvFlags.MDB_NORDAHEAD)
            .build());
    }

    @Test
    public void testOf() {
        final EnvFlags envFlag = EnvFlags.MDB_FIXEDMAP;
        final EnvFlagSet envFlagSet = EnvFlagSet.of(envFlag);
        assertThat(envFlagSet.getMask()).isEqualTo(MaskedFlag.mask(envFlag));
        assertThat(envFlagSet.size()).isEqualTo(1);
        assertThat(envFlagSet.isSet(EnvFlags.MDB_NOSUBDIR)).isEqualTo(false);
        for (EnvFlags flag : envFlagSet) {
            assertThat(envFlagSet.isSet(flag)).isEqualTo(true);
        }

        final EnvFlagSet envFlagSet2 = EnvFlagSet.builder()
            .setFlag(envFlag)
            .build();
        assertThat(envFlagSet).isEqualTo(envFlagSet2);
    }

    @Test
    public void testOf2() {
        final EnvFlags envFlag1 = EnvFlags.MDB_FIXEDMAP;
        final EnvFlags envFlag2 = EnvFlags.MDB_NORDAHEAD;
        final EnvFlagSet envFlagSet = EnvFlagSet.of(envFlag1, envFlag2);
        assertThat(envFlagSet.getMask()).isEqualTo(MaskedFlag.mask(envFlag1, envFlag2));
        assertThat(envFlagSet.size()).isEqualTo(2);
        assertThat(envFlagSet.isSet(EnvFlags.MDB_WRITEMAP)).isEqualTo(false);
        for (EnvFlags flag : envFlagSet) {
            assertThat(envFlagSet.isSet(flag)).isEqualTo(true);
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
        assertThat(envFlagSet.getMask()).isEqualTo(MaskedFlag.mask(envFlag1, envFlag2));
        assertThat(envFlagSet.size()).isEqualTo(2);
        assertThat(envFlagSet.isSet(EnvFlags.MDB_NOTLS)).isEqualTo(false);
        for (EnvFlags flag : envFlagSet) {
            assertThat(envFlagSet.isSet(flag)).isEqualTo(true);
        }
        final EnvFlagSet envFlagSet2 = EnvFlagSet.builder()
                .withFlags(envFlag1, envFlag2)
                .build();
        final EnvFlagSet envFlagSet3 = EnvFlagSet.builder()
                .withFlags(new HashSet<>(Arrays.asList(envFlag1, envFlag2)))
                .build();
        assertThat(envFlagSet).isEqualTo(envFlagSet2);
        assertThat(envFlagSet).isEqualTo(envFlagSet3);
    }
}
