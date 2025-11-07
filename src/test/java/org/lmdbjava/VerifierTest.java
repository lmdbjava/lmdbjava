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
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Test {@link Verifier}. */
public final class VerifierTest {

  @Test
  void verification() {
    try (final TempDir tempDir = new TempDir()) {
      final Path file = tempDir.createTempFile();
      try (Env<ByteBuffer> env =
          create()
              .setMaxReaders(1)
              .setMaxDbs(Verifier.DBI_COUNT)
              .setMapSize(10, ByteUnit.MEBIBYTES)
              .setEnvFlags(MDB_NOSUBDIR)
              .open(file)) {
        final Verifier v = new Verifier(env);
        final int seconds = Integer.getInteger("verificationSeconds", 2);
        assertThat(v.runFor(seconds, TimeUnit.SECONDS)).isGreaterThan(1L);
      }
    }
  }
}
