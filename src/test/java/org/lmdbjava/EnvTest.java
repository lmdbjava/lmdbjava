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
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.AlreadyOpenException;
import org.lmdbjava.Env.Builder;
import org.lmdbjava.Env.InvalidCopyDestination;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.Txn.BadReaderLockException;

/**
 * Test {@link Env}.
 */
public final class EnvTest {

  @Test
  void byteUnit() {
    FileUtil.useTempFile(
        file -> {
          try (Env<ByteBuffer> env = Env.create()
              .setMaxReaders(1)
              .setMapSize(1, ByteUnit.MEBIBYTES)
              .setEnvFlags(MDB_NOSUBDIR)
              .open(file)) {
            final EnvInfo info = env.info();
            assertThat(info.mapSize).isEqualTo(ByteUnit.MEBIBYTES.toBytes(1));
          }
        });
  }

  @Test
  void cannotChangeMapSizeAfterOpen() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Builder<ByteBuffer> builder = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR);
                try (Env<ByteBuffer> ignored = builder.open(file)) {
                  builder.setMapSize(1);
                }
              });
        })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotChangeMaxDbsAfterOpen() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Builder<ByteBuffer> builder = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR);
                try (Env<ByteBuffer> ignored = builder.open(file)) {
                  builder.setMaxDbs(1);
                }
              });
        })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotChangeMaxReadersAfterOpen() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Builder<ByteBuffer> builder = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR);
                try (Env<ByteBuffer> ignored = builder.open(file)) {
                  builder.setMaxReaders(1);
                }
              });
        })
        .isInstanceOf(AlreadyOpenException.class);
  }

  @Test
  void cannotInfoOnceClosed() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Env<ByteBuffer> env = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR)
                    .open(file);
                env.close();
                env.info();
              });
        })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotOpenTwice() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Builder<ByteBuffer> builder = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR);
                builder.open(file).close();
                //noinspection resource // This will fail to open
                builder.open(file);
              });
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
  void cannotStatOnceClosed() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Env<ByteBuffer> env = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR)
                    .open(file);
                env.close();
                env.stat();
              });
        })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void cannotSyncOnceClosed() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                final Env<ByteBuffer> env = Env.create()
                    .setMaxReaders(1)
                    .setEnvFlags(MDB_NOSUBDIR)
                    .open(file);
                env.close();
                env.sync(false);
              });
        })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void copyDirectoryBased() {
    FileUtil.useTempDir(
        dest -> {
          assertThat(Files.exists(dest)).isTrue();
          assertThat(Files.isDirectory(dest)).isTrue();
          assertThat(FileUtil.count(dest)).isEqualTo(0);
          FileUtil.useTempDir(
              src -> {
                try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
                  env.copy(dest.toFile(), MDB_CP_COMPACT);
                  assertThat(FileUtil.count(dest)).isEqualTo(1);
                }
              });
        });
  }

  @Test
  void copyDirectoryRejectsFileDestination() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempDir(
              dest -> {
                FileUtil.deleteDir(dest);
                FileUtil.useTempDir(
                    src -> {
                      try (Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(src)) {
                        env.copy(dest.toFile(), MDB_CP_COMPACT);
                      }
                    });
              });
        })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyDirectoryRejectsMissingDestination() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempDir(
              dest -> {
                try {
                  Files.delete(dest);
                  FileUtil.useTempDir(
                      src -> {
                        try (Env<ByteBuffer> env =
                                 Env.create().setMaxReaders(1).open(src)) {
                          env.copy(dest.toFile(), MDB_CP_COMPACT);
                        }
                      });
                } catch (final IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
        })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyDirectoryRejectsNonEmptyDestination() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempDir(
              dest -> {
                try {
                  final Path subDir = dest.resolve("hello");
                  Files.createDirectory(subDir);
                  assertThat(Files.isDirectory(subDir)).isTrue();
                  FileUtil.useTempDir(
                      src -> {
                        try (Env<ByteBuffer> env =
                                 Env.create().setMaxReaders(1).open(src)) {
                          env.copy(dest.toFile(), MDB_CP_COMPACT);
                        }
                      });
                } catch (final IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
        })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void copyFileBased() {
    FileUtil.useTempFile(
        dest -> {
          FileUtil.delete(dest);
          assertThat(Files.exists(dest)).isFalse();
          FileUtil.useTempFile(
              src -> {
                try (Env<ByteBuffer> env =
                         Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(src)) {
                  env.copy(dest.toFile(), MDB_CP_COMPACT);
                }
                assertThat(FileUtil.size(dest)).isGreaterThan(0L);
              });
        });
  }

  @Test
  void copyFileRejectsExistingDestination() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              dest -> {
                assertThat(Files.exists(dest)).isTrue();
                FileUtil.useTempFile(
                    src -> {
                      try (Env<ByteBuffer> env =
                               Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(src)) {
                        env.copy(dest.toFile(), MDB_CP_COMPACT);
                      }
                    });
              });
        })
        .isInstanceOf(InvalidCopyDestination.class);
  }

  @Test
  void createAsDirectory() {
    FileUtil.useTempDir(
        dest -> {
          final Env<ByteBuffer> env = Env.create().setMaxReaders(1).open(dest);
          assertThat(Files.isDirectory(dest)).isTrue();
          env.sync(false);
          env.close();
          assertThat(env.isClosed()).isTrue();
          env.close(); // safe to repeat
        });
  }

  @Test
  void createAsFile() {
    FileUtil.useTempFile(
        file -> {
          try (Env<ByteBuffer> env = Env.create()
              .setMapSize(1, ByteUnit.MEBIBYTES)
              .setMaxDbs(1)
              .setMaxReaders(1)
              .setEnvFlags(MDB_NOSUBDIR)
              .open(file)) {
            env.sync(true);
            assertThat(Files.isRegularFile(file)).isTrue();
          }
        });
  }

  @Test
  void detectTransactionThreadViolation() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempFile(
              file -> {
                try (Env<ByteBuffer> env =
                         Env.create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR).open(file)) {
                  env.txnRead();
                  env.txnRead();
                }
              });
        })
        .isInstanceOf(BadReaderLockException.class);
  }

  @Test
  void info() {
    FileUtil.useTempFile(
        file -> {
          try (Env<ByteBuffer> env =
                   Env.create().setMaxReaders(4).setMapSize(123_456).setEnvFlags(MDB_NOSUBDIR).open(file)) {
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
        });
  }

  @Test
  void mapFull() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempDir(
              dir -> {
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
                  final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
                  //noinspection InfiniteLoopStatement // Needs infinite loop to fill the env
                  for (; ; ) {
                    rnd.nextBytes(k);
                    key.clear();
                    key.put(k).flip();
                    val.clear();
                    db.put(key, val);
                  }
                }
              });
        })
        .isInstanceOf(MapFullException.class);
  }

  @Test
  void readOnlySupported() {
    FileUtil.useTempDir(
        dir -> {
          try (Env<ByteBuffer> rwEnv = Env.create()
              .setMaxReaders(1)
              .open(dir)) {
            final Dbi<ByteBuffer> rwDb = rwEnv.openDbi(DB_1, MDB_CREATE);
            rwDb.put(bb(1), bb(42));
          }
          try (Env<ByteBuffer> roEnv = Env.create()
              .setMaxReaders(1)
              .setEnvFlags(MDB_RDONLY_ENV)
              .open(dir)) {
            final Dbi<ByteBuffer> roDb = roEnv.openDbi(DB_1, DbiFlagSet.EMPTY);
            try (Txn<ByteBuffer> roTxn = roEnv.txnRead()) {
              assertThat(roDb.get(roTxn, bb(1))).isNotNull();
            }
          }
        });
  }

  @Test
  void setMapSize() {
    FileUtil.useTempDir(
        dir -> {
          final byte[] k = new byte[500];
          final ByteBuffer key = allocateDirect(500);
          final ByteBuffer val = allocateDirect(1_024);
          final Random rnd = new Random();
          try (Env<ByteBuffer> env = Env.create()
              .setMaxReaders(1)
              .setMapSize(256, ByteUnit.KIBIBYTES)
              .setMaxDbs(1)
              .open(dir)) {
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
        });
  }

  @Test
  void stats() {
    FileUtil.useTempFile(
        file -> {
          try (Env<ByteBuffer> env = Env.create()
              .setMaxReaders(1)
              .setEnvFlags(MDB_NOSUBDIR)
              .open(file)) {
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
        });
  }

  @Test
  void testDefaultOpen() {
    FileUtil.useTempDir(
        dir -> {
          try (Env<ByteBuffer> env = Env.create().setMapSize(10, ByteUnit.MEBIBYTES).open(dir)) {
            final EnvInfo info = env.info();
            assertThat(info.maxReaders).isEqualTo(MAX_READERS_DEFAULT);
            final Dbi<ByteBuffer> db = env.openDbi("test", MDB_CREATE);
            db.put(allocateDirect(1), allocateDirect(1));
          }
        });
  }
}
