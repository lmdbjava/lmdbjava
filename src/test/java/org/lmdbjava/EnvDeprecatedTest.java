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

import static java.nio.ByteBuffer.allocateDirect;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.ByteUnit.KIBIBYTES;
import static org.lmdbjava.ByteUnit.MEBIBYTES;
import static org.lmdbjava.CopyFlags.MDB_CP_COMPACT;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.Builder.MAX_READERS_DEFAULT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.AlreadyOpenException;
import org.lmdbjava.Env.Builder;
import org.lmdbjava.Env.InvalidCopyDestination;
import org.lmdbjava.Env.MapFullException;

/**
 * Tests all the deprecated methods in {@link Env}. Essentially a duplicate of {@link EnvTest}. When
 * all the deprecated methods are deleted we can delete this test class.
 *
 * @deprecated Tests all the deprecated methods in {@link Env}.
 */
@Deprecated
public class EnvDeprecatedTest {

  private TempDir tempDir;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
  }

  @AfterEach
  void afterEach() {
    tempDir.cleanup();
  }

  @Test
  void byteUnit() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMaxReaders(1)
            .setMapSize(MEBIBYTES.toBytes(1))
            .open(file.toFile(), MDB_NOSUBDIR)) {
      final EnvInfo info = env.info();
      assertThat(info.mapSize).isEqualTo(MEBIBYTES.toBytes(1));
    }
  }

  @Test
  void cannotChangeMapSizeAfterOpen() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              try (Env<ByteBuffer> env = builder.open(file.toFile(), MDB_NOSUBDIR)) {
                builder.setMapSize(1);
              }
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotChangeMaxDbsAfterOpen() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              try (Env<ByteBuffer> env = builder.open(file.toFile(), MDB_NOSUBDIR)) {
                builder.setMaxDbs(1);
              }
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotChangeMaxReadersAfterOpen() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              try (Env<ByteBuffer> env = builder.open(file.toFile(), MDB_NOSUBDIR)) {
                builder.setMaxReaders(1);
              }
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotInfoOnceClosed() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).open(file.toFile(), MDB_NOSUBDIR);
              env.close();
              env.info();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotOpenTwice() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              builder.open(file.toFile(), MDB_NOSUBDIR).close();
              builder.open(file.toFile(), MDB_NOSUBDIR);
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotStatOnceClosed() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).open(file.toFile(), MDB_NOSUBDIR);
              env.close();
              env.stat();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotSyncOnceClosed() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).open(file.toFile(), MDB_NOSUBDIR);
              env.close();
              env.sync(false);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void copyDirectoryBased() {
    final Path dest = tempDir.createTempDir();
    final Path src = tempDir.createTempDir();
    assertThat(Files.exists(dest)).isTrue();
    assertThat(Files.isDirectory(dest)).isTrue();
    assertThat(FileUtil.count(dest)).isEqualTo(0);
    try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src.toFile())) {
      env.copy(dest.toFile(), MDB_CP_COMPACT);
      assertThat(FileUtil.count(dest)).isEqualTo(1);
    }
  }

  @Test
  void copyDirectoryRejectsFileDestination() {
    assertThatThrownBy(
            () -> {
              final Path dest = tempDir.createTempDir();
              final Path src = tempDir.createTempDir();
              FileUtil.deleteDir(dest);
              try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src.toFile())) {
                env.copy(dest.toFile(), MDB_CP_COMPACT);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyDirectoryRejectsMissingDestination() {
    final Path dest = tempDir.createTempDir();
    final Path src = tempDir.createTempDir();
    assertThatThrownBy(
            () -> {
              try {
                Files.delete(dest);
                try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src.toFile())) {
                  env.copy(dest.toFile(), MDB_CP_COMPACT);
                }
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyDirectoryRejectsNonEmptyDestination() {
    final Path dest = tempDir.createTempDir();
    final Path src = tempDir.createTempDir();
    assertThatThrownBy(
            () -> {
              try {
                final Path subDir = dest.resolve("hello");
                Files.createDirectory(subDir);
                assertThat(Files.isDirectory(subDir)).isTrue();
                try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src.toFile())) {
                  env.copy(dest.toFile(), MDB_CP_COMPACT);
                }
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyFileBased() {
    final Path dest = tempDir.createTempFile();
    final Path src = tempDir.createTempFile();
    assertThat(Files.exists(dest)).isFalse();
    try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src.toFile(), MDB_NOSUBDIR)) {
      env.copy(dest.toFile(), MDB_CP_COMPACT);
    }
    assertThat(FileUtil.size(dest)).isGreaterThan(0L);
  }

  @Test
  void copyFileRejectsExistingDestination() {
    final Path dest = tempDir.createTempFile();
    final Path src = tempDir.createTempFile();
    assertThatThrownBy(
            () -> {
              Files.createFile(dest);
              assertThat(Files.exists(dest)).isTrue();
              try (Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).open(src.toFile(), MDB_NOSUBDIR)) {
                env.copy(dest.toFile(), MDB_CP_COMPACT);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void createAsFile() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(MEBIBYTES.toBytes(1))
            .setMaxDbs(1)
            .setMaxReaders(1)
            .open(file.toFile(), MDB_NOSUBDIR)) {
      env.sync(true);
      assertThat(Files.isRegularFile(file)).isTrue();
    }
  }

  @Test
  void mapFull() {
    final Path dir = tempDir.createTempDir();
    assertThatThrownBy(
            () -> {
              final byte[] k = new byte[500];
              final ByteBuffer key = allocateDirect(500);
              final ByteBuffer val = allocateDirect(1_024);
              final Random rnd = new Random();
              try (Env<ByteBuffer> env =
                  Env.create()
                      .setMaxReaders(1)
                      .setMapSize(MEBIBYTES.toBytes(8))
                      .setMaxDbs(1)
                      .open(dir.toFile())) {
                final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
                for (; ; ) {
                  rnd.nextBytes(k);
                  key.clear();
                  key.put(k).flip();
                  val.clear();
                  db.put(key, val);
                }
              }
            })
        .isInstanceOf(MapFullException.class);
  }

  @Test
  void readOnlySupported() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> rwEnv = Env.create().setMaxReaders(1).open(dir.toFile())) {
      final Dbi<ByteBuffer> rwDb = rwEnv.openDbi(DB_1, MDB_CREATE);
      rwDb.put(bb(1), bb(42));
    }
    try (Env<ByteBuffer> roEnv = Env.create().setMaxReaders(1).open(dir.toFile(), MDB_RDONLY_ENV)) {
      final Dbi<ByteBuffer> roDb = roEnv.openDbi(DB_1);
      try (Txn<ByteBuffer> roTxn = roEnv.txnRead()) {
        assertThat(roDb.get(roTxn, bb(1))).isNotNull();
      }
    }
  }

  @Test
  void setMapSize() {
    final Path dir = tempDir.createTempDir();
    final byte[] k = new byte[500];
    final ByteBuffer key = allocateDirect(500);
    final ByteBuffer val = allocateDirect(1_024);
    final Random rnd = new Random();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMaxReaders(1)
            .setMapSize(KIBIBYTES.toBytes(256))
            .setMaxDbs(1)
            .open(dir.toFile())) {
      final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

      db.put(bb(1), bb(42));
      boolean mapFullExThrown = false;
      try {
        for (int i = 0; i < 70; i++) {
          rnd.nextBytes(k);
          key.clear();
          key.put(k).flip();
          val.clear();
          db.put(key, val);
        }
      } catch (final MapFullException mfE) {
        mapFullExThrown = true;
      }
      assertThat(mapFullExThrown).isTrue();

      env.setMapSize(KIBIBYTES.toBytes(1024));

      try (Txn<ByteBuffer> roTxn = env.txnRead()) {
        final ByteBuffer byteBuffer = db.get(roTxn, bb(1));
        assertThat(byteBuffer).isNotNull();
        assertThat(byteBuffer.getInt()).isEqualTo(42);
      }

      mapFullExThrown = false;
      try {
        for (int i = 0; i < 70; i++) {
          rnd.nextBytes(k);
          key.clear();
          key.put(k).flip();
          val.clear();
          db.put(key, val);
        }
      } catch (final MapFullException mfE) {
        mapFullExThrown = true;
      }
      assertThat(mapFullExThrown).isFalse();
    }
  }

  @Test
  void testDefaultOpen() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.open(dir.toFile(), 10)) {
      final EnvInfo info = env.info();
      assertThat(info.maxReaders).isEqualTo(MAX_READERS_DEFAULT);
      final Dbi<ByteBuffer> db = env.openDbi("test", MDB_CREATE);
      db.put(allocateDirect(1), allocateDirect(1));
    }
  }
}
