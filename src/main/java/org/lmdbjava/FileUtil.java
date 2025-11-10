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
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.stream.Stream;

final class FileUtil {

  private FileUtil() {}

  static long size(final Path path) {
    try {
      return Files.size(path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static void deleteFile(final Path path) {
    try {
      Files.delete(path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static void deleteDir(final Path path) {
    if (path != null && Files.isDirectory(path)) {
      recursiveDelete(path);
      deleteIfExists(path);
    }
  }

  private static void recursiveDelete(final Path path) {
    try {
      Files.walkFileTree(
          path,
          EnumSet.of(FileVisitOption.FOLLOW_LINKS),
          Integer.MAX_VALUE,
          new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(
                final Path dir, final BasicFileAttributes attrs) throws IOException {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(final Path file, final IOException exc)
                throws IOException {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
              deleteIfExists(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) {
              if (!dir.equals(path)) {
                deleteIfExists(dir);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (final NotDirectoryException e) {
      // Ignore.
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void deleteIfExists(final Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static long count(final Path path) {
    try (final Stream<Path> stream = Files.list(path)) {
      return stream.count();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
