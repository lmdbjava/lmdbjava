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
import static org.lmdbjava.LmdbNativeException.PageCorruptedException.MDB_CORRUPTED;
import static org.lmdbjava.Meta.error;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

import org.junit.jupiter.api.Test;
import org.lmdbjava.Meta.Version;

/** Test {@link Meta}. */
public final class MetaTest {

  @Test
  public void coverPrivateConstructors() {
    invokePrivateConstructor(Meta.class);
  }

  @Test
  void errCode() {
    assertThat(error(MDB_CORRUPTED)).isEqualTo("MDB_CORRUPTED: Located page was wrong type");
  }

  @Test
  void version() {
    final Version v = Meta.version();
    assertThat(v).isNotNull();
    assertThat(v.major).isEqualTo(0);
    assertThat(v.minor).isEqualTo(9);
  }
}
