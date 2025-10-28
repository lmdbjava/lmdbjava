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

public class DbiFlagSetTest {

    @Test
    public void testEmpty() {
        final DbiFlagSet dbiFlagSet = DbiFlagSet.empty();
        assertThat(
                dbiFlagSet.getMask(),
                is(0));
        assertThat(
                dbiFlagSet.size(),
                is(0));
        assertThat(
                dbiFlagSet.isEmpty(),
                is(true));
        assertThat(
                dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        final DbiFlagSet dbiFlagSet2 = DbiFlagSet.builder()
            .build();
        assertThat(dbiFlagSet, is(dbiFlagSet2));
        assertThat(dbiFlagSet, not(DbiFlagSet.of(DbiFlags.MDB_CREATE)));
        assertThat(dbiFlagSet, not(DbiFlagSet.of(DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT)));
        assertThat(dbiFlagSet, not(DbiFlagSet.builder()
            .setFlag(DbiFlags.MDB_CREATE)
            .setFlag(DbiFlags.MDB_DUPFIXED)
            .build()));
    }

    @Test
    public void testOf() {
        final DbiFlags dbiFlag = DbiFlags.MDB_CREATE;
        final DbiFlagSet dbiFlagSet = DbiFlagSet.of(dbiFlag);
        assertThat(
                dbiFlagSet.getMask(),
                is(MaskedFlag.mask(dbiFlag)));
        assertThat(
                dbiFlagSet.size(),
                is(1));
        assertThat(
                dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        for (DbiFlags flag : dbiFlagSet) {
            assertThat(
                    dbiFlagSet.isSet(flag),
                    is(true));
        }

        final DbiFlagSet dbiFlagSet2 = DbiFlagSet.builder()
            .setFlag(dbiFlag)
            .build();
        assertThat(dbiFlagSet, is(dbiFlagSet2));
    }

    @Test
    public void testOf2() {
        final DbiFlags dbiFlag1 = DbiFlags.MDB_CREATE;
        final DbiFlags dbiFlag2 = DbiFlags.MDB_INTEGERKEY;
        final DbiFlagSet dbiFlagSet = DbiFlagSet.of(dbiFlag1, dbiFlag2);
        assertThat(
                dbiFlagSet.getMask(),
                is(MaskedFlag.mask(dbiFlag1, dbiFlag2)));
        assertThat(
                dbiFlagSet.size(),
                is(2));
        assertThat(
                dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        for (DbiFlags flag : dbiFlagSet) {
            assertThat(
                    dbiFlagSet.isSet(flag),
                    is(true));
        }
    }

    @Test
    public void testBuilder() {
        final DbiFlags dbiFlag1 = DbiFlags.MDB_CREATE;
        final DbiFlags dbiFlag2 = DbiFlags.MDB_INTEGERKEY;
        final DbiFlagSet dbiFlagSet = DbiFlagSet.builder()
                .setFlag(dbiFlag1)
                .setFlag(dbiFlag2)
                .build();
        assertThat(
                dbiFlagSet.getMask(),
                is(MaskedFlag.mask(dbiFlag1, dbiFlag2)));
        assertThat(
                dbiFlagSet.size(),
                is(2));
        assertThat(
                dbiFlagSet.isSet(DbiFlags.MDB_REVERSEDUP),
                is(false));
        for (DbiFlags flag : dbiFlagSet) {
            assertThat(
                    dbiFlagSet.isSet(flag),
                    is(true));
        }
        final DbiFlagSet dbiFlagSet2 = DbiFlagSet.builder()
                .withFlags(dbiFlag1, dbiFlag2)
                .build();
        final DbiFlagSet dbiFlagSet3 = DbiFlagSet.builder()
                .withFlags(new HashSet<>(Arrays.asList(dbiFlag1, dbiFlag2)))
                .build();
        assertThat(dbiFlagSet, is(dbiFlagSet2));
        assertThat(dbiFlagSet, is(dbiFlagSet3));
    }
}
