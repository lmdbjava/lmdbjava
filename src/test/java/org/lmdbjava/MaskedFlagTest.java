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
import static org.lmdbjava.EnvFlags.MDB_FIXEDMAP;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;

import org.junit.jupiter.api.Test;

/** Test {@link MaskedFlag}. */
public final class MaskedFlagTest {

  @Test
  void isSetOperates() {
    assertThat(isSet(0, MDB_NOSYNC)).isFalse();
    assertThat(isSet(0, MDB_FIXEDMAP)).isFalse();
    assertThat(isSet(0, MDB_RDONLY_ENV)).isFalse();

    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_NOSYNC)).isFalse();
    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_FIXEDMAP)).isTrue();
    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_RDONLY_ENV)).isFalse();

    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_NOSYNC)).isTrue();
    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_FIXEDMAP)).isFalse();
    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_RDONLY_ENV)).isFalse();

    final int syncFixed = mask(MDB_NOSYNC, MDB_FIXEDMAP);
    assertThat(isSet(syncFixed, MDB_NOSYNC)).isTrue();
    assertThat(isSet(syncFixed, MDB_FIXEDMAP)).isTrue();
    assertThat(isSet(syncFixed, MDB_RDONLY_ENV)).isFalse();
  }

  @Test
  void masking() {
    final EnvFlags[] nullFlags = null;
    assertThat(mask(nullFlags)).isEqualTo(0);

    final EnvFlags[] emptyFlags = new EnvFlags[] {};
    assertThat(mask(emptyFlags)).isEqualTo(0);

    final EnvFlags[] nullElementZero = new EnvFlags[] {null};
    assertThat(nullElementZero.length).isEqualTo(1);
    assertThat(mask(nullElementZero)).isEqualTo(0);

    assertThat(mask(MDB_NOSYNC)).isEqualTo(MDB_NOSYNC.getMask());

    final int expected = MDB_NOSYNC.getMask() + MDB_FIXEDMAP.getMask();
    assertThat(mask(MDB_NOSYNC, MDB_FIXEDMAP)).isEqualTo(expected);
  }
}
