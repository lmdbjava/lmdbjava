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

import org.junit.Test;
import org.lmdbjava.Lmdb.MDB_envinfo;

import java.lang.foreign.Arena;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

/**
 * Test {@link Library}.
 */
public final class LibraryTest {

  @Test
  public void coverPrivateConstructors() {
    invokePrivateConstructor(Library.class);
  }

  @Test
  public void structureFieldOrder() {
    try (final Arena arena = Arena.ofConfined()) {
      final MDB_envinfo v = new MDB_envinfo(arena);
      assertThat(v.meMapaddr().address(), is(0L));
      assertThat(v.meMapsize(), is(0L));
    }
  }
}
