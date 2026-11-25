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
import static org.lmdbjava.CopyFlags.MDB_CP_COMPACT;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.Builder.MAX_READERS_DEFAULT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_NOTLS;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.AlreadyOpenException;
import org.lmdbjava.Env.Builder;
import org.lmdbjava.Env.InvalidCopyDestination;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.Txn.BadReaderLockException;

/** Test {@link Env}. */
public final class EnvTest {

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
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file)) {
      final EnvInfo info = env.info();
      assertThat(info.mapSize).isEqualTo(ByteUnit.MEBIBYTES.toBytes(1));
    }
  }

  @Test
  void cannotChangeMapSizeAfterOpen() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR);
              try (Env<ByteBuffer> ignored = builder.setEnvFlags(MDB_NOSUBDIR).open(file)) {
                builder.setMapSize(1);
              }
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotChangePermissionsAfterOpen() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder =
                  Env.create().setFilePermissions(0666).setEnvFlags(MDB_NOSUBDIR);
              try (Env<ByteBuffer> ignored = builder.setEnvFlags(MDB_NOSUBDIR).open(file)) {
                builder.setFilePermissions(0664);
              }
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotChangeMaxDbsAfterOpen() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Builder<ByteBuffer> builder =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR);
              try (Env<ByteBuffer> ignored = builder.setEnvFlags(MDB_NOSUBDIR).open(file)) {
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
              final Builder<ByteBuffer> builder =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR);
              try (Env<ByteBuffer> ignored = builder.setEnvFlags(MDB_NOSUBDIR).open(file)) {
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
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
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
              final Builder<ByteBuffer> builder =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR);
              builder.open(file).close();
              //noinspection resource // This will fail to open
              builder.open(file);
            })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotOverflowMapSize() {
    assertThatThrownBy(
            () -> {
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              final int mb = 1_024 * 1_024;
              //noinspection NumericOverflow // Intentional overflow
              final int size = mb * 2_048; // as per issue 18
              builder.setMapSize(size);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void negativeMapSize() {
    assertThatThrownBy(
            () -> {
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              builder.setMapSize(-1);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void negativeMapSize2() {
    assertThatThrownBy(
            () -> {
              final Builder<ByteBuffer> builder = Env.create().setMaxReaders(1);
              builder.setMapSize(-1, ByteUnit.MEBIBYTES);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void cannotStatOnceClosed() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              final Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
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
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
              env.close();
              env.sync(false);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void copyDirectoryBased() {
    final Path dest = tempDir.createTempDir();
    assertThat(Files.exists(dest)).isTrue();
    assertThat(Files.isDirectory(dest)).isTrue();
    assertThat(FileUtil.count(dest)).isEqualTo(0);
    final Path src = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
      assertThat(FileUtil.count(dest)).isEqualTo(1);
    }
  }

  @Test
  void copyDirectoryBased_noFlags() {
    final Path dest = tempDir.createTempDir();
    assertThat(Files.exists(dest)).isTrue();
    assertThat(Files.isDirectory(dest)).isTrue();
    assertThat(FileUtil.count(dest)).isEqualTo(0);
    final Path src = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
      env.copy(dest);
      assertThat(FileUtil.count(dest)).isEqualTo(1);
    }
  }

  @Test
  void copyDirectoryRejectsFileDestination() {
    assertThatThrownBy(
            () -> {
              final Path dest = tempDir.createTempDir();
              FileUtil.deleteDir(dest);
              final Path src = tempDir.createTempDir();
              try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
                env.copy(dest, MDB_CP_COMPACT);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyDirectoryRejectsMissingDestination() {
    assertThatThrownBy(
            () -> {
              final Path dest = tempDir.createTempDir();
              try {
                Files.delete(dest);
                final Path src = tempDir.createTempDir();
                try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
                  env.copy(dest, MDB_CP_COMPACT);
                }
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyDirectoryRejectsNonEmptyDestination() {
    assertThatThrownBy(
            () -> {
              final Path dest = tempDir.createTempDir();
              try {
                final Path subDir = dest.resolve("hello");
                Files.createDirectory(subDir);
                assertThat(Files.isDirectory(subDir)).isTrue();
                final Path src = tempDir.createTempDir();
                try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
                  env.copy(dest, MDB_CP_COMPACT);
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
    assertThat(Files.exists(dest)).isFalse();
    final Path src = tempDir.createTempFile();
    try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
    assertThat(FileUtil.size(dest)).isGreaterThan(0L);
  }

  @Test
  void copyFileRejectsExistingDestination() {
    assertThatThrownBy(
            () -> {
              final Path dest = tempDir.createTempFile();
              Files.createFile(dest);
              assertThat(Files.exists(dest)).isTrue();
              final Path src = tempDir.createTempFile();
              try (Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(src)) {
                env.copy(dest, MDB_CP_COMPACT);
              }
            })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void createAsDirectory() {
    final Path dest = tempDir.createTempDir();
    final Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(dest);
    assertThat(Files.isDirectory(dest)).isTrue();
    env.sync(false);
    env.close();
    assertThat(env.isClosed()).isTrue();
    env.close(); // safe to repeat
  }

  @Test
  void createAsFile() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file)) {
      env.sync(true);
      assertThat(Files.isRegularFile(file)).isTrue();
    }
  }

  @Test
  void detectTransactionThreadViolation() {
    assertThatThrownBy(
            () -> {
              final Path file = tempDir.createTempFile();
              try (Env<ByteBuffer> env =
                  Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file)) {
                env.txnRead();
                env.txnRead();
              }
            })
        .isInstanceOf(BadReaderLockException.class);
  }

  @Test
  void info() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMaxReaders(4)
            .setMapSize(123_456)
            .setEnvFlags(MDB_NOSUBDIR)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file)) {
      final EnvInfo info = env.info();
      assertThat(info).isNotNull();
      assertThat(info.lastPageNumber).isEqualTo(1L);
      assertThat(info.lastTransactionId).isEqualTo(0L);
      assertThat(info.mapAddress).isEqualTo(0L);
      assertThat(info.mapSize).isEqualTo(123_456L);
      assertThat(info.maxReaders).isEqualTo(4);
      assertThat(info.numReaders).isEqualTo(0);
      assertThat(info.toString()).contains("maxReaders=");
      assertThat(env.getMaxKeySize()).isEqualTo(511);
    }
  }

  @Test
  void mapFull() {
    assertThatThrownBy(
            () -> {
              final Path dir = tempDir.createTempDir();
              final byte[] k = new byte[500];
              final ByteBuffer key = allocateDirect(500);
              final ByteBuffer val = allocateDirect(1_024);
              final Random rnd = new Random();
              try (Env<ByteBuffer> env =
                  Env.create()
                      .setMaxReaders(1)
                      .setMapSize(8, ByteUnit.MEBIBYTES)
                      .setMaxDbs(1)
                      .open(dir)) {
                final Dbi<ByteBuffer> db =
                    env.createDbi()
                        .setDbName(DB_1)
                        .withDefaultComparator()
                        .setDbiFlags(MDB_CREATE)
                        .open();
                //noinspection InfiniteLoopStatement // Needs infinite loop to fill the env
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
    try (Env<ByteBuffer> rwEnv = Env.create().setMaxReaders(1).open(dir)) {
      final Dbi<ByteBuffer> rwDb =
          rwEnv.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();
      rwDb.put(bb(1), bb(42));
    }
    try (Env<ByteBuffer> roEnv =
        Env.create().setMaxReaders(1).setEnvFlags(MDB_RDONLY_ENV).open(dir)) {
      final Dbi<ByteBuffer> roDb =
          roEnv
              .createDbi()
              .setDbName(DB_1)
              .withDefaultComparator()
              .setDbiFlags(DbiFlagSet.EMPTY)
              .open();
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
        Env.create().setMaxReaders(1).setMapSize(256, ByteUnit.KIBIBYTES).setMaxDbs(1).open(dir)) {
      final Dbi<ByteBuffer> db =
          env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();

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

      assertThatThrownBy(
              () -> {
                env.setMapSize(-1, ByteUnit.KIBIBYTES);
              })
          .isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(
              () -> {
                env.setMapSize(-1);
              })
          .isInstanceOf(IllegalArgumentException.class);

      env.setMapSize(1024, ByteUnit.KIBIBYTES);

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
  void stats() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file)) {
      final Stat stat = env.stat();
      assertThat(stat).isNotNull();
      assertThat(stat.branchPages).isEqualTo(0L);
      assertThat(stat.depth).isEqualTo(0);
      assertThat(stat.entries).isEqualTo(0L);
      assertThat(stat.leafPages).isEqualTo(0L);
      assertThat(stat.overflowPages).isEqualTo(0L);
      assertThat(stat.pageSize % 4_096).isEqualTo(0);
      assertThat(stat.toString()).contains("pageSize=");
    }
  }

  @Test
  void testDefaultOpen() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
      final EnvInfo info = env.info();
      assertThat(info.maxReaders).isEqualTo(MAX_READERS_DEFAULT);
      final Dbi<ByteBuffer> db =
          env.createDbi().setDbName("test").withDefaultComparator().setDbiFlags(MDB_CREATE).open();
      db.put(allocateDirect(1), allocateDirect(1));
    }
  }

  @Test
  void testDefaultOpenNoName1() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
      final EnvInfo info = env.info();
      assertThat(info.maxReaders).isEqualTo(MAX_READERS_DEFAULT);
      final Dbi<ByteBuffer> db =
          env.createDbi()
              .setDbName((String) null)
              .withDefaultComparator()
              .setDbiFlags(MDB_CREATE)
              .open();
      db.put(bb("abc"), allocateDirect(1));
      db.put(bb("def"), allocateDirect(1));

      // As this is the unnamed database it returns all keys in the unnamed db
      final List<byte[]> dbiNamesBytes = env.getDbiNames();
      assertThat(dbiNamesBytes).hasSize(2);
      assertThat(dbiNamesBytes.get(0)).isEqualTo("abc".getBytes(Env.DEFAULT_NAME_CHARSET));
      assertThat(dbiNamesBytes.get(1)).isEqualTo("def".getBytes(Env.DEFAULT_NAME_CHARSET));

      final List<String> dbiNames = env.getDbiNames(Env.DEFAULT_NAME_CHARSET);
      assertThat(dbiNames).hasSize(2);
      assertThat(dbiNames.get(0)).isEqualTo("abc");
      assertThat(dbiNames.get(1)).isEqualTo("def");
    }
  }

  @Test
  void testDefaultOpenNoName2() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
      final EnvInfo info = env.info();
      assertThat(info.maxReaders).isEqualTo(MAX_READERS_DEFAULT);
      final Dbi<ByteBuffer> db =
          env.createDbi()
              .setDbName((byte[]) null)
              .withDefaultComparator()
              .setDbiFlags(MDB_CREATE)
              .open();
      db.put(bb("abc"), allocateDirect(1));
      db.put(bb("def"), allocateDirect(1));

      // As this is the unnamed database it returns all keys in the unnamed db
      final List<byte[]> dbiNames = env.getDbiNames();
      assertThat(dbiNames).hasSize(2);
      assertThat(dbiNames.get(0)).isEqualTo("abc".getBytes(Env.DEFAULT_NAME_CHARSET));
      assertThat(dbiNames.get(1)).isEqualTo("def".getBytes(Env.DEFAULT_NAME_CHARSET));
    }
  }

  @Test
  void addEnvFlag() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .addEnvFlag(MDB_NOSUBDIR)
            .addEnvFlag(MDB_NOTLS) // Should not overwrite the existing one
            .open(file)) {
      env.sync(true);
      assertThat(Files.isRegularFile(file)).isTrue();
      assertThat(env.getEnvFlagSet().getFlags())
          .containsExactlyInAnyOrderElementsOf(EnvFlagSet.of(MDB_NOSUBDIR, MDB_NOTLS).getFlags());
    }
  }

  @Test
  void addEnvFlags() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .addEnvFlags(EnvFlagSet.of(MDB_NOSUBDIR, MDB_NOTLS))
            .addEnvFlag(MDB_NOTLS) // Should not overwrite the existing one
            .addEnvFlag(null) // no-op
            .addEnvFlags((EnvFlagSet) null) // no-op
            .addEnvFlags((Collection<EnvFlags>) null) // no-op
            .open(file)) {
      env.sync(true);
      assertThat(env.getEnvFlagSet().getFlags())
          .containsExactlyInAnyOrderElementsOf(EnvFlagSet.of(MDB_NOSUBDIR, MDB_NOTLS).getFlags());
      assertThat(Files.isRegularFile(file)).isTrue();
    }
  }

  @Test
  void addEnvFlags2() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .addEnvFlags(Arrays.asList(MDB_NOSUBDIR, MDB_NOTLS))
            .addEnvFlags(Collections.singleton(MDB_NOSYNC))
            .open(file)) {
      env.sync(true);
      assertThat(env.getEnvFlagSet().getFlags())
          .containsExactlyInAnyOrderElementsOf(
              EnvFlagSet.of(MDB_NOSUBDIR, MDB_NOTLS, MDB_NOSYNC).getFlags());
      assertThat(Files.isRegularFile(file)).isTrue();
    }
  }

  @Test
  void setEnvFlags() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags((EnvFlagSet) null) // No-op
            .setEnvFlags((EnvFlags) null) // No-op
            .setEnvFlags((EnvFlags[]) null) // No-op
            .setEnvFlags((Collection<EnvFlags>) null) // No-op
            .setEnvFlags(MDB_NOSYNC) // Will be overwritten
            .setEnvFlags(Arrays.asList(MDB_NOSUBDIR, MDB_NOTLS))
            .open(file)) {
      env.sync(true);
      assertThat(Files.isRegularFile(file)).isTrue();
      assertThat(env.getEnvFlagSet().getFlags())
          .containsExactlyInAnyOrderElementsOf(EnvFlagSet.of(MDB_NOSUBDIR, MDB_NOTLS).getFlags());
    }
  }

  @Test
  void setEnvFlags2() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR, MDB_NOTLS)
            .setEnvFlags(Collections.emptySet()) // Clears them
            .open(dir)) {
      env.sync(true);
      assertThat(env.getEnvFlagSet().getFlags()).isEmpty();
      assertThat(Files.isDirectory(dir));
    }
  }

  @Test
  void setEnvFlags_null1() {
    final Path file = tempDir.createTempFile();
    // MDB_NOSUBDIR is cleared out so it will error as file is a file not a dir
    Assertions.assertThatThrownBy(
            () -> {
              try (Env<ByteBuffer> env =
                  Env.create()
                      .setMapSize(1, ByteUnit.MEBIBYTES)
                      .setMaxDbs(1)
                      .setMaxReaders(1)
                      .addEnvFlag(MDB_NOSUBDIR)
                      .setEnvFlags((Collection<EnvFlags>) null) // Clears the flags
                      .open(file)) {}
            })
        .isInstanceOf(LmdbNativeException.class);
  }

  @Test
  void setEnvFlags_null2() {
    final Path file = tempDir.createTempFile();
    // MDB_NOSUBDIR is cleared out so it will error as file is a file not a dir
    Assertions.assertThatThrownBy(
            () -> {
              try (Env<ByteBuffer> env =
                  Env.create()
                      .setMapSize(1, ByteUnit.MEBIBYTES)
                      .setMaxDbs(1)
                      .setMaxReaders(1)
                      .addEnvFlag(MDB_NOSUBDIR)
                      .setEnvFlags((EnvFlags) null) // Clears the flags
                      .open(file)) {}
            })
        .isInstanceOf(LmdbNativeException.class);
  }

  @Test
  void setEnvFlags_null3() {
    final Path file = tempDir.createTempFile();
    // MDB_NOSUBDIR is cleared out so it will error as file is a file not a dir
    Assertions.assertThatThrownBy(
            () -> {
              try (Env<ByteBuffer> env =
                  Env.create()
                      .setMapSize(1, ByteUnit.MEBIBYTES)
                      .setMaxDbs(1)
                      .setMaxReaders(1)
                      .addEnvFlag(MDB_NOSUBDIR)
                      .setEnvFlags((EnvFlagSet) null) // Clears the flags
                      .open(file)) {}
            })
        .isInstanceOf(LmdbNativeException.class);
  }
}
