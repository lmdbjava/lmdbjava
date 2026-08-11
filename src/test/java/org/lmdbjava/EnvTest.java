/*
 * Copyright © 2016-2026 The LmdbJava Open Source Project
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
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
            .setSafeClose()
            .setMaxReaders(1)
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file)) {
      final EnvInfo info = env.info();
      assertThat(info.mapSize).isEqualTo(ByteUnit.MEBIBYTES.toBytes(1));
    }
  }

  @Test
  void cannotChangeBuilderAfterOpen() {
    final Path file = tempDir.createTempFile();
    final Builder<ByteBuffer> builder =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR);
    try (Env<ByteBuffer> ignored = builder.open(file)) {

      // Now try to modify the builder after it has been used to open an Env
      assertThatThrownBy(() -> builder.setMapSize(1)).isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(builder::setSafeClose).isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setSafeClose(true)).isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(builder::setSingleThreaded).isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setSingleThreaded(true))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setEnvFlags(EnvFlagSet.of(MDB_NOSUBDIR)))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setMaxReaders(1)).isInstanceOf(AlreadyOpenException.class);
      //noinspection OctalInteger
      assertThatThrownBy(() -> builder.setFilePermissions(0666))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setMaxDbs(1)).isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setMapSize(1, ByteUnit.MEBIBYTES))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.addEnvFlag(MDB_NOSYNC))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.addEnvFlags(EnvFlagSet.of(MDB_NOSYNC)))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.addEnvFlags(Collections.singleton(MDB_NOSYNC)))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setEnvFlags(MDB_NOSYNC))
          .isInstanceOf(AlreadyOpenException.class);
      assertThatThrownBy(() -> builder.setEnvFlags(Collections.singleton(MDB_NOSYNC)))
          .isInstanceOf(AlreadyOpenException.class);
      //noinspection resource
      assertThatThrownBy(() -> builder.open(file)).isInstanceOf(AlreadyOpenException.class);
    }
  }

  @Test
  void cannotInfoOnceClosed() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
    env.close();
    assertThatThrownBy(env::info).isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotOverflowMapSize() {
    assertThatThrownBy(
            () -> {
              final Builder<ByteBuffer> builder = Env.create().setSafeClose().setMaxReaders(1);
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
              final Builder<ByteBuffer> builder = Env.create().setSafeClose().setMaxReaders(1);
              builder.setMapSize(-1);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void negativeMapSize2() {
    assertThatThrownBy(
            () -> {
              final Builder<ByteBuffer> builder = Env.create().setSafeClose().setMaxReaders(1);
              builder.setMapSize(-1, ByteUnit.MEBIBYTES);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void cannotStatOnceClosed() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
    env.close();
    assertThatThrownBy(env::stat).isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotSyncOnceClosed() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
    env.close();
    assertThatThrownBy(() -> env.sync(false)).isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotOpenReadTxnOnceClosed() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
    env.close();
    assertThatThrownBy(env::txnRead).isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotOpenWriteTxnOnceClosed() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
    env.close();
    assertThatThrownBy(env::txnWrite).isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotOpenTxnOnceClosed() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file);
    env.close();
    assertThatThrownBy(() -> env.txn(null)).isInstanceOf(AlreadyClosedException.class);
    assertThatThrownBy(() -> env.txn(null, TxnFlags.MDB_RDONLY_TXN))
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void copyDirectoryBased() {
    final Path dest = tempDir.createTempDir();
    assertThat(Files.exists(dest)).isTrue();
    assertThat(Files.isDirectory(dest)).isTrue();
    assertThat(FileUtil.count(dest)).isEqualTo(0);
    final Path src = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setSafeClose().setMaxReaders(1).open(src)) {
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
    try (Env<ByteBuffer> env = Env.create().setSafeClose().setMaxReaders(1).open(src)) {
      env.copy(dest);
      assertThat(FileUtil.count(dest)).isEqualTo(1);
    }
  }

  @Test
  void copyDirectoryRejectsFileDestination() {
    final Path dest = tempDir.createTempDir();
    FileUtil.deleteDir(dest);
    final Path src = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setSafeClose().setMaxReaders(1).open(src)) {
      assertThatThrownBy(() -> env.copy(dest, MDB_CP_COMPACT))
          .isInstanceOf(InvalidCopyDestination.class);
    }
  }

  @Test
  void copyDirectoryRejectsMissingDestination() {
    final Path dest = tempDir.createTempDir();
    try {
      Files.delete(dest);
      final Path src = tempDir.createTempDir();
      try (Env<ByteBuffer> env = Env.create().setSafeClose().setMaxReaders(1).open(src)) {
        assertThatThrownBy(() -> env.copy(dest, MDB_CP_COMPACT))
            .isInstanceOf(InvalidCopyDestination.class);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void copyDirectoryRejectsNonEmptyDestination() throws IOException {
    final Path dest = tempDir.createTempDir();
    final Path subDir = dest.resolve("hello");
    Files.createDirectory(subDir);
    assertThat(Files.isDirectory(subDir)).isTrue();
    final Path src = tempDir.createTempDir();
    try (Env<ByteBuffer> env = Env.create().setSafeClose().setMaxReaders(1).open(src)) {
      assertThatThrownBy(() -> env.copy(dest, MDB_CP_COMPACT))
          .isInstanceOf(InvalidCopyDestination.class);
    }
  }

  @Test
  void copyFileBased() {
    final Path dest = tempDir.createTempFile();
    assertThat(Files.exists(dest)).isFalse();
    final Path src = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
    assertThat(FileUtil.size(dest)).isGreaterThan(0L);
  }

  @Test
  void copyFileRejectsExistingDestination() throws IOException {
    final Path dest = tempDir.createTempFile();
    Files.createFile(dest);
    assertThat(Files.exists(dest)).isTrue();
    final Path src = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(src)) {
      assertThatThrownBy(() -> env.copy(dest, MDB_CP_COMPACT))
          .isInstanceOf(InvalidCopyDestination.class);
    }
  }

  @Test
  void createAsDirectory() {
    final Path dest = tempDir.createTempDir();
    final Env<ByteBuffer> env = Env.create().setSafeClose().setMaxReaders(1).open(dest);
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
            .setSafeClose()
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
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(3).setEnvFlags(MDB_NOSUBDIR).open(file)) {
      try (Txn<ByteBuffer> ignored = env.txnRead()) {
        // When NOT using MDB_NOTLS flag, you cannot open a second read txn on the same thread
        assertThatThrownBy(env::txnRead).isInstanceOf(BadReaderLockException.class);
      }
    }
  }

  @Test
  void multipleReadTxnsOnSameThread() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMaxReaders(3)
            .setEnvFlags(MDB_NOSUBDIR, MDB_NOTLS)
            .open(file)) {
      try (Txn<ByteBuffer> ignored1 = env.txnRead()) {
        // MDB_NOTLS flag allows us to open multiple read txns on the same thread
        //noinspection EmptyTryBlock
        try (Txn<ByteBuffer> ignored2 = env.txnRead()) {}
      }
    }
  }

  @Test
  void info() {
    final Path file = tempDir.createTempFile();
    try (Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
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
    final Path dir = tempDir.createTempDir();
    final byte[] k = new byte[500];
    final ByteBuffer key = allocateDirect(500);
    final ByteBuffer val = allocateDirect(1_024);
    final Random rnd = new Random();
    try (Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMaxReaders(1)
            .setMapSize(8, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .open(dir)) {
      final Dbi<ByteBuffer> db =
          env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();
      assertThatThrownBy(
              () -> {
                // Fill the env until MapFullException is thrown
                for (; ; ) {
                  rnd.nextBytes(k);
                  key.clear();
                  key.put(k).flip();
                  val.clear();
                  db.put(key, val);
                }
              })
          .isInstanceOf(MapFullException.class);
    }
  }

  @Test
  void readOnlySupported() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> rwEnv = Env.create().setSafeClose().setMaxReaders(1).open(dir)) {
      final Dbi<ByteBuffer> rwDb =
          rwEnv.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();
      rwDb.put(bb(1), bb(42));
    }
    try (Env<ByteBuffer> roEnv =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_RDONLY_ENV).open(dir)) {
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
        Env.create()
            .setSafeClose()
            .setMaxReaders(1)
            .setMapSize(256, ByteUnit.KIBIBYTES)
            .setMaxDbs(1)
            .open(dir)) {
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

      assertThatThrownBy(() -> env.setMapSize(-1, ByteUnit.KIBIBYTES))
          .isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(() -> env.setMapSize(-1)).isInstanceOf(IllegalArgumentException.class);

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
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file)) {
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
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
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
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
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
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
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
    try (final Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
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
            .setSafeClose()
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
            .setSafeClose()
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
            .setSafeClose()
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
            .setSafeClose()
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
    // MDB_NOSUBDIR is cleared out, so it will error as file is a file not a dir
    Assertions.assertThatThrownBy(
            () -> {
              //noinspection EmptyTryBlock
              try (final Env<ByteBuffer> ignored =
                  Env.create()
                      .setSafeClose()
                      .setMapSize(1, ByteUnit.MEBIBYTES)
                      .setMaxDbs(1)
                      .setMaxReaders(1)
                      .addEnvFlag(MDB_NOSUBDIR)
                      .setEnvFlags((Collection<EnvFlags>) null) // Clears the flags
                      .open(file)) {}
            })
        .isInstanceOf(LmdbNativeException.class)
        .hasMessageContaining("No such file or directory");
  }

  @Test
  void setEnvFlags_null2() {
    final Path file = tempDir.createTempFile();
    // MDB_NOSUBDIR is cleared out so it will error as file is a file not a dir
    Assertions.assertThatThrownBy(
            () -> {
              //noinspection EmptyTryBlock
              try (Env<ByteBuffer> ignored =
                  Env.create()
                      .setSafeClose()
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
              //noinspection EmptyTryBlock
              try (Env<ByteBuffer> ignored =
                  Env.create()
                      .setSafeClose()
                      .setMapSize(1, ByteUnit.MEBIBYTES)
                      .setMaxDbs(1)
                      .setMaxReaders(1)
                      .addEnvFlag(MDB_NOSUBDIR)
                      .setEnvFlags((EnvFlagSet) null) // Clears the flags
                      .open(file)) {}
            })
        .isInstanceOf(LmdbNativeException.class);
  }

  @Test
  void closeWithOpenReadTxn() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);

    // Open but don't close
    final Txn<ByteBuffer> readTxn = env.txnWrite();

    Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);

    readTxn.close();
    env.close();
  }

  @Test
  void tryCloseWithOpenReadTxn() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);

    // Open but don't close
    final Txn<ByteBuffer> readTxn = env.txnWrite();

    assertThat(env.tryClose()).isFalse();
    readTxn.close();
    assertThat(env.tryClose()).isTrue();
    // already closed
    assertThat(env.tryClose()).isFalse();
  }

  @Test
  void immediateClose() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);

    env.close();
    // no-op
    env.close();
  }

  @Test
  void immediateTryClose() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);

    assertThat(env.tryClose()).isTrue();
    // already closed
    assertThat(env.tryClose()).isFalse();
  }

  @Test
  void closeWithOpenWriteTxn() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setSafeClose()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);

    // Open but don't close
    final Txn<ByteBuffer> writeTxn = env.txnWrite();

    Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);

    writeTxn.close();
    env.close();
  }

  @Test
  void closeWithOpenCursor() {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR)
            .setSafeClose(true)
            .setSingleThreaded(true)
            .open(file);

    final Dbi<ByteBuffer> dbi =
        env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();

    // Open but don't close
    final Txn<ByteBuffer> writeTxn = env.txnWrite();
    final Cursor<ByteBuffer> cursor = dbi.openCursor(writeTxn);
    // Close the txn but not the cursor
    writeTxn.close();

    Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);

    Assertions.assertThatThrownBy(cursor::close).isInstanceOf(Txn.NotReadyException.class);

    // can't close the env as we are unable to close the cursor
  }

  /**
   * Regression for the intermittent close-during-read SIGSEGV (lmdbjava#253 / lmdbjava#279). With
   * safe close enabled, {@link Env#close()} must never unmap the memory map while another thread is
   * still inside a live read transaction; instead it fails fast with {@link Env.EnvInUseException}.
   *
   * <p>Unlike the {@code RefCounter} unit tests, this exercises the real {@code Env}/{@code Txn}
   * wiring against native LMDB: many threads hammer {@code txnRead()}/{@code Dbi.get} while another
   * thread races {@link Env#close()}. On {@code master} (no safe close) this reliably crashes the
   * JVM in {@code mdb_txn_renew0}; with safe close the close is rejected while reads are in flight,
   * readers only ever observe {@link Env.AlreadyClosedException}, and the env closes cleanly once
   * the readers stop.
   */
  @Test
  void closeDuringConcurrentReads_isRejectedWhileReadersLiveAndSurvives() throws Exception {
    final Path dir = tempDir.createTempDir();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(64).setMaxDbs(1).open(dir);
    final Dbi<ByteBuffer> db =
        env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();
    for (int i = 0; i < 32; i++) {
      db.put(bb(i), bb(i));
    }

    final int readerCount = 16;
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong reads = new AtomicLong();
    final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    final List<Thread> readers = new ArrayList<>(readerCount);

    for (int i = 0; i < readerCount; i++) {
      final int seed = i;
      final Thread reader =
          new Thread(
              () -> {
                int k = seed;
                while (!stop.get()) {
                  try (Txn<ByteBuffer> txn = env.txnRead()) {
                    db.get(txn, bb(k & 31));
                    reads.incrementAndGet();
                    k++;
                  } catch (final AlreadyClosedException expected) {
                    return; // benign: env is closing/closed
                  } catch (final Throwable t) {
                    unexpected.add(t);
                    return;
                  }
                }
              },
              "reader-" + seed);
      reader.setDaemon(true);
      reader.start();
      readers.add(reader);
    }

    // A transaction held on this thread guarantees the count is non-zero, so the racing close()
    // below deterministically fails fast rather than unmapping. The hammer threads meanwhile race
    // real native txn begin/renew against that close().
    try (Txn<ByteBuffer> ignoredHeldReader = env.txnRead()) {
      Thread.sleep(100); // let the reader threads saturate the native read path
      Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);
      assertThat(env.tryClose()).isFalse();
      assertThat(env.isClosed()).isFalse(); // must NOT have unmapped with live readers
    }

    // Stop the hammer threads and wait for every in-flight transaction to be released.
    stop.set(true);
    for (final Thread reader : readers) {
      reader.join(5_000);
    }

    // With no live transactions the env now closes cleanly.
    env.close();

    assertThat(reads.get()).isGreaterThan(0L);
    assertThat(unexpected).isEmpty();
    assertThat(env.isClosed()).isTrue();
  }

  /**
   * As {@link #closeDuringConcurrentReads_isRejectedWhileReadersLiveAndSurvives()} but the readers
   * additionally open a {@link Cursor} on each transaction. Safe close newly tracks cursors as well
   * as transactions, so this covers the cursor acquire/release wiring under a concurrent close
   * race, which the existing single-threaded cursor test does not.
   */
  @Test
  void closeDuringConcurrentCursorReads_isRejectedWhileCursorsLiveAndSurvives() throws Exception {
    final Path dir = tempDir.createTempDir();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(64).setMaxDbs(1).open(dir);
    final Dbi<ByteBuffer> db =
        env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();
    for (int i = 0; i < 32; i++) {
      db.put(bb(i), bb(i));
    }

    final int readerCount = 16;
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong reads = new AtomicLong();
    final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    final List<Thread> readers = new ArrayList<>(readerCount);

    for (int i = 0; i < readerCount; i++) {
      final Thread reader =
          new Thread(
              () -> {
                while (!stop.get()) {
                  try (Txn<ByteBuffer> txn = env.txnRead();
                      Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
                    cursor.first();
                    reads.incrementAndGet();
                  } catch (final AlreadyClosedException expected) {
                    return; // benign: env is closing/closed
                  } catch (final Throwable t) {
                    unexpected.add(t);
                    return;
                  }
                }
              },
              "cursor-reader-" + i);
      reader.setDaemon(true);
      reader.start();
      readers.add(reader);
    }

    // A cursor held on this thread guarantees a non-zero count during the racing close().
    final Txn<ByteBuffer> heldReader = env.txnRead();
    final Cursor<ByteBuffer> heldCursor = db.openCursor(heldReader);
    try {
      Thread.sleep(100);
      Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);
      assertThat(env.tryClose()).isFalse();
      assertThat(env.isClosed()).isFalse();
    } finally {
      heldCursor.close();
      heldReader.close();
    }

    stop.set(true);
    for (final Thread reader : readers) {
      reader.join(5_000);
    }

    env.close();

    assertThat(reads.get()).isGreaterThan(0L);
    assertThat(unexpected).isEmpty();
    assertThat(env.isClosed()).isTrue();
  }

  @Test
  void testEventualTryClose() throws InterruptedException {
    final Path dir = tempDir.createTempDir();
    final Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(64).setMaxDbs(1).open(dir);
    final Dbi<ByteBuffer> db =
        env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();
    for (int i = 0; i < 32; i++) {
      db.put(bb(i), bb(i));
    }

    final int readerCount = 16;
    final AtomicLong reads = new AtomicLong();
    final LongAdder completedCount = new LongAdder();
    final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    final List<Thread> readers = new ArrayList<>(readerCount);

    final Instant endTime = Instant.now().plusMillis(500);

    // Have 16 threads all hammer the env with reads until timeout
    for (int i = 0; i < readerCount; i++) {
      final Thread reader =
          new Thread(
              () -> {
                while (Instant.now().isBefore(endTime)) {
                  try (Txn<ByteBuffer> txn = env.txnRead();
                      Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
                    cursor.first();
                    reads.incrementAndGet();
                  } catch (final AlreadyClosedException expected) {
                    return; // benign: env is closing/closed
                  } catch (final Throwable t) {
                    unexpected.add(t);
                    return;
                  }
                }
                completedCount.increment();
              },
              "cursor-reader-" + i);
      reader.setDaemon(true);
      reader.start();
      readers.add(reader);
    }

    // Keep trying to close until we are able
    boolean didClose = false;
    while (!didClose) {
      Thread.sleep(10);
      didClose = env.tryClose();
      if (didClose) {
        assertThat(completedCount).hasValue(readerCount);
      }
    }

    // readers should all have completed by now anyway
    for (final Thread reader : readers) {
      reader.join(5_000);
    }

    assertThat(env.tryClose()).isFalse();
    assertThat(reads.get()).isGreaterThan(0L);
    assertThat(unexpected).isEmpty();
    assertThat(completedCount).hasValue(readerCount);
    assertThat(env.isClosed()).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "TRUE, TRUE",
    "TRUE, FALSE",
    "FALSE, TRUE",
    "FALSE, FALSE",
    "NO_ARG, NO_ARG",
    "NOT_CALLED, NOT_CALLED"
  })
  void singleThreaded(final BooleanArg safeClose, final BooleanArg singleThreaded) {
    testEnvUse(safeClose, singleThreaded);
  }

  @Test
  void testToString() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> env =
        Env.create().setSafeClose().setMaxReaders(64).setMaxDbs(1).open(dir)) {
      assertThat(env.toString()).doesNotStartWith("@");
    }
  }

  private void testEnvUse(final BooleanArg safeClose, final BooleanArg singleThreaded) {
    final Path file = tempDir.createTempFile();

    final Builder<ByteBuffer> builder =
        Env.create()
            .setMapSize(1, ByteUnit.MEBIBYTES)
            .setMaxDbs(1)
            .setMaxReaders(1)
            .setEnvFlags(MDB_NOSUBDIR);

    switch (safeClose) {
      case TRUE:
      case FALSE:
        builder.setSafeClose(safeClose.getAsBoolean());
        // Handle no argument case
        break;
      case NO_ARG:
        builder.setSafeClose();
        break;
      case NOT_CALLED:
        // Don't call anything
        break;
    }

    switch (singleThreaded) {
      case TRUE:
      case FALSE:
        builder.setSingleThreaded(singleThreaded.getAsBoolean());
        // Handle no argument case
        break;
      case NO_ARG:
        builder.setSingleThreaded();
        break;
      case NOT_CALLED:
        // Don't call anything
        break;
    }

    try (Env<ByteBuffer> env = builder.open(file)) {
      assertThat(env.isSafeClose()).isEqualTo(safeClose.getAsBoolean());
      assertThat(env.isSingleThreaded()).isEqualTo(singleThreaded.getAsBoolean());

      final Dbi<ByteBuffer> dbi =
          env.createDbi().setDbName(DB_1).withDefaultComparator().setDbiFlags(MDB_CREATE).open();

      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        for (int i = 0; i < 10; i++) {
          dbi.put(txn, bb(i), bb(100 + i), MDB_APPENDDUP);

          if (safeClose.getAsBoolean()) {
            Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);
          }
        }
        txn.commit();
      }

      for (int i = 0; i < 5; i++) {
        try (Txn<ByteBuffer> txn = env.txnRead();
            Cursor<ByteBuffer> cursor = dbi.openCursor(txn)) {
          int j = 0;
          while (cursor.next()) {
            final KeyVal<ByteBuffer> keyVal = cursor.keyVal();
            Assertions.assertThat(keyVal.key().getInt()).isEqualTo(j);
            Assertions.assertThat(keyVal.val().getInt()).isEqualTo(100 + j);
            if (safeClose.getAsBoolean()) {
              Assertions.assertThatThrownBy(env::close).isInstanceOf(Env.EnvInUseException.class);
            }
            j++;
          }
        }
      }
    }
  }

  private enum BooleanArg {
    TRUE(true),
    FALSE(false),
    NO_ARG(true),
    NOT_CALLED(false); // Both safeClose and singleThreaded default to false if not set

    private final boolean isTrue;

    BooleanArg(final boolean isTrue) {
      this.isTrue = isTrue;
    }

    boolean getAsBoolean() {
      return isTrue;
    }
  }
}
