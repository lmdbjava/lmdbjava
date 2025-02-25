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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.TargetName.isExternal;
import static org.lmdbjava.TargetName.resolveFilename;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

import org.junit.Test;

/** Test {@link TargetName}. */
public final class TargetNameTest {

  private static final String NONE = "";

  @Test
  public void coverPrivateConstructors() {
    invokePrivateConstructor(TargetName.class);
  }

  @Test
  public void customEmbedded() {
    assertThat(resolveFilename(NONE, "x/y.so", NONE, NONE), is("x/y.so"));
    assertThat(isExternal(NONE), is(false));
  }

  @Test
  public void embeddedNameResolution() {
    embed("aarch64-linux-gnu.so", "aarch64", "Linux");
    embed("aarch64-macos-none.so", "aarch64", "Mac OS");
    embed("x86_64-linux-gnu.so", "x86_64", "Linux");
    embed("x86_64-macos-none.so", "x86_64", "Mac OS");
    embed("x86_64-windows-gnu.dll", "x86_64", "Windows");
  }

  @Test
  public void externalLibrary() {
    assertThat(resolveFilename("/l.so", NONE, NONE, NONE), is("/l.so"));
    assertThat(TargetName.isExternal("/l.so"), is(true));
  }

  @Test
  public void externalTakesPriority() {
    assertThat(resolveFilename("/lm.so", "x/y.so", NONE, NONE), is("/lm.so"));
    assertThat(isExternal("/lm.so"), is(true));
  }

  private void embed(final String lib, final String arch, final String os) {
    assertThat(resolveFilename(NONE, NONE, arch, os), is("org/lmdbjava/" + lib));
    assertThat(isExternal(NONE), is(false));
  }
}
