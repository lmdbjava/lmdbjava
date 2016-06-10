/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static java.lang.Long.BYTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import org.lmdbjava.Library.MDB_envinfo;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

public class LibraryTest {

  @Test
  public void coverPrivateConstructors() throws Exception {
    invokePrivateConstructor(Library.class);
  }

  @Test
  public void structureFieldOrder() throws Exception {
    MDB_envinfo v = new MDB_envinfo(RUNTIME);
    assertThat(v.f0_me_mapaddr.offset(), is(0L));
    assertThat(v.f1_me_mapsize.offset(), is((long) BYTES));
  }
}
