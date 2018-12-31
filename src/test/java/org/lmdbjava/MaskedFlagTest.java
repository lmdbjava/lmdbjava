/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2019 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import org.junit.Test;
import static org.lmdbjava.EnvFlags.MDB_FIXEDMAP;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;

/**
 * Test {@link MaskedFlag}.
 */
public final class MaskedFlagTest {

  @Test
  public void isSetOperates() {
    assertThat(isSet(0, MDB_NOSYNC), is(false));
    assertThat(isSet(0, MDB_FIXEDMAP), is(false));
    assertThat(isSet(0, MDB_RDONLY_ENV), is(false));

    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_NOSYNC), is(false));
    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_FIXEDMAP), is(true));
    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_RDONLY_ENV), is(false));

    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_NOSYNC), is(true));
    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_FIXEDMAP), is(false));
    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_RDONLY_ENV), is(false));

    final int syncFixed = mask(MDB_NOSYNC, MDB_FIXEDMAP);
    assertThat(isSet(syncFixed, MDB_NOSYNC), is(true));
    assertThat(isSet(syncFixed, MDB_FIXEDMAP), is(true));
    assertThat(isSet(syncFixed, MDB_RDONLY_ENV), is(false));
  }

  @Test
  public void masking() {
    final EnvFlags[] nullFlags = null;
    assertThat(mask(nullFlags), is(0));

    final EnvFlags[] emptyFlags = new EnvFlags[]{};
    assertThat(mask(emptyFlags), is(0));

    final EnvFlags[] nullElementZero = new EnvFlags[]{null};
    assertThat(nullElementZero, is(arrayWithSize(1)));
    assertThat(mask(nullElementZero), is(0));

    assertThat(mask(MDB_NOSYNC), is(MDB_NOSYNC.getMask()));

    final int expected = MDB_NOSYNC.getMask() + MDB_FIXEDMAP.getMask();
    assertThat(mask(MDB_NOSYNC, MDB_FIXEDMAP), is(expected));
  }
}
