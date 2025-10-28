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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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
        final PutFlagSet putFlagSet2 = PutFlagSet.builder()
            .build();
        assertThat(putFlagSet, is(putFlagSet2));
        assertThat(putFlagSet, not(PutFlagSet.of(PutFlags.MDB_APPEND)));
        assertThat(putFlagSet, not(PutFlagSet.of(PutFlags.MDB_APPEND, PutFlags.MDB_RESERVE)));
        assertThat(putFlagSet, not(PutFlagSet.builder()
            .setFlag(PutFlags.MDB_CURRENT)
            .setFlag(PutFlags.MDB_MULTIPLE)
            .build()));
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

        final PutFlagSet putFlagSet2 = PutFlagSet.builder()
            .setFlag(putFlag)
            .build();
        assertThat(putFlagSet, is(putFlagSet2));
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
