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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class TempDir implements AutoCloseable {
  private final Path root;

  public TempDir() {
    try {
      root = Files.createTempDirectory("lmdb");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Path createTempFile() {
    return root.resolve(UUID.randomUUID().toString());
  }

  public Path createTempDir() {
    try {
      final Path dir = root.resolve(UUID.randomUUID().toString());
      Files.createDirectory(dir);
      return dir;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void cleanup() {
    FileUtil.deleteDir(root);
  }

  @Override
  public void close() {
    cleanup();
  }
}
