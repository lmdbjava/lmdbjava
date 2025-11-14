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

import static java.lang.Long.BYTES;
import static java.lang.Long.MIN_VALUE;
import static java.nio.ByteBuffer.allocateDirect;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteUnit.MEBIBYTES;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPFIXED;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_MULTIPLE;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_GET_BOTH;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Txn.NotReadyException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;

/**
 * Tests all the deprecated methods in {@link Cursor}. Essentially a duplicate of {@link
 * CursorTest}. When all the deprecated methods are deleted we can delete this test class.
 *
 * @deprecated Tests all the deprecated methods in {@link Cursor}.
 */
@Deprecated
public class CursorDeprecatedTest {

  private TempDir tempDir;
  private Env<ByteBuffer> env;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
    Path file = tempDir.createTempFile();
    env =
        create(PROXY_OPTIMAL)
            .setMapSize(MEBIBYTES.toBytes(1))
            .setMaxReaders(1)
            .setMaxDbs(1)
            .open(file.toFile(), Env.Builder.POSIX_MODE_DEFAULT, MDB_NOSUBDIR);
  }

  @AfterEach
  void afterEach() {
    env.close();
    tempDir.cleanup();
  }

  @Test
  void count() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), new PutFlags[] {MDB_APPENDDUP});
      assertThat(c.count()).isEqualTo(1L);
      c.put(bb(1), bb(4), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(1), bb(6), new PutFlags[] {MDB_APPENDDUP});
      assertThat(c.count()).isEqualTo(3L);
      c.put(bb(2), bb(1), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(2), bb(2), new PutFlags[] {MDB_APPENDDUP});
      assertThat(c.count()).isEqualTo(2L);
    }
  }

  @Test
  void cursorCannotCloseIfTransactionCommitted() {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
              try (Txn<ByteBuffer> txn = env.txnWrite()) {
                try (Cursor<ByteBuffer> c = db.openCursor(txn); ) {
                  c.put(bb(1), bb(2), new PutFlags[] {MDB_APPENDDUP});
                  assertThat(c.count()).isEqualTo(1L);
                  c.put(bb(1), bb(4), new PutFlags[] {MDB_APPENDDUP});
                  assertThat(c.count()).isEqualTo(2L);
                  txn.commit();
                }
              }
            })
        .isInstanceOf(NotReadyException.class);
  }

  @Test
  void cursorFirstLastNextPrev() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), new PutFlags[] {MDB_NOOVERWRITE});
      c.put(bb(3), bb(4), new PutFlags[0]);
      c.put(bb(5), bb(6), new PutFlags[0]);
      c.put(bb(7), bb(8), new PutFlags[0]);

      assertThat(c.first()).isTrue();
      assertThat(c.key().getInt(0)).isEqualTo(1);
      assertThat(c.val().getInt(0)).isEqualTo(2);

      assertThat(c.last()).isTrue();
      assertThat(c.key().getInt(0)).isEqualTo(7);
      assertThat(c.val().getInt(0)).isEqualTo(8);

      assertThat(c.prev()).isTrue();
      assertThat(c.key().getInt(0)).isEqualTo(5);
      assertThat(c.val().getInt(0)).isEqualTo(6);

      assertThat(c.first()).isTrue();
      assertThat(c.next()).isTrue();
      assertThat(c.key().getInt(0)).isEqualTo(3);
      assertThat(c.val().getInt(0)).isEqualTo(4);
    }
  }

  @Test
  void delete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4), new PutFlags[0]);
      assertThat(c.seek(MDB_FIRST)).isTrue();
      assertThat(c.key().getInt()).isEqualTo(1);
      assertThat(c.val().getInt()).isEqualTo(2);
      c.delete();
      assertThat(c.seek(MDB_FIRST)).isTrue();
      assertThat(c.key().getInt()).isEqualTo(3);
      assertThat(c.val().getInt()).isEqualTo(4);
      c.delete();
      assertThat(c.seek(MDB_FIRST)).isFalse();
    }
  }

  @Test
  void getKeyVal() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(1), bb(4), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(1), bb(6), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(2), bb(1), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(2), bb(2), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(2), bb(3), new PutFlags[] {MDB_APPENDDUP});
      c.put(bb(2), bb(4), new PutFlags[] {MDB_APPENDDUP});
      assertThat(c.get(bb(1), bb(2), MDB_GET_BOTH)).isTrue();
      assertThat(c.count()).isEqualTo(3L);
      assertThat(c.get(bb(1), bb(3), MDB_GET_BOTH)).isFalse();
      assertThat(c.get(bb(2), bb(1), MDB_GET_BOTH)).isTrue();
      assertThat(c.count()).isEqualTo(4L);
      assertThat(c.get(bb(2), bb(0), MDB_GET_BOTH)).isFalse();
    }
  }

  @Test
  void putMultiple() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT, MDB_DUPFIXED);
    final int elemCount = 20;

    final ByteBuffer values = allocateDirect(Integer.BYTES * elemCount);
    for (int i = 1; i <= elemCount; i++) {
      values.putInt(i);
    }
    values.flip();

    final int key = 100;
    final ByteBuffer k = bb(key);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.putMultiple(k, values, elemCount, new PutFlags[] {MDB_MULTIPLE});
      assertThat(c.count()).isEqualTo((long) elemCount);
    }
  }

  @Test
  void putMultipleWithoutMdbMultipleFlag() {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
              try (Txn<ByteBuffer> txn = env.txnWrite();
                  Cursor<ByteBuffer> c = db.openCursor(txn)) {
                c.putMultiple(bb(100), bb(1), 1, new PutFlags[0]);
              }
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void renewTxRw() {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
              try (Txn<ByteBuffer> txn = env.txnWrite()) {
                assertThat(txn.isReadOnly()).isFalse();

                try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
                  c.renew(txn);
                }
              }
            })
        .isInstanceOf(ReadOnlyRequiredException.class);
  }

  @Test
  void repeatedCloseCausesNotError() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.close();
      c.close();
    }
  }

  @Test
  void reserve() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    final ByteBuffer key = bb(5);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(db.get(txn, key)).isNull();
      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
        final ByteBuffer val = c.reserve(key, BYTES * 2, new PutFlags[0]);
        assertThat(db.get(txn, key)).isNotNull();
        val.putLong(MIN_VALUE).flip();
      }
      txn.commit();
    }
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val.capacity()).isEqualTo(BYTES * 2);
      assertThat(val.getLong()).isEqualTo(MIN_VALUE);
    }
  }

  @Test
  void returnValueForNoDupData() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      // ok
      assertThat(c.put(bb(5), bb(6), new PutFlags[] {MDB_NODUPDATA})).isTrue();
      assertThat(c.put(bb(5), bb(7), new PutFlags[] {MDB_NODUPDATA})).isTrue();
      assertThat(c.put(bb(5), bb(6), new PutFlags[] {MDB_NODUPDATA})).isFalse();
    }
  }

  @Test
  void returnValueForNoOverwrite() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      // ok
      assertThat(c.put(bb(5), bb(6), new PutFlags[] {MDB_NOOVERWRITE})).isTrue();
      // fails, but gets exist val
      assertThat(c.put(bb(5), bb(8), new PutFlags[] {MDB_NOOVERWRITE})).isFalse();
      assertThat(c.val().getInt(0)).isEqualTo(6);
    }
  }

  @Test
  void testCursorByteBufferDuplicate() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
        c.put(bb(1), bb(2), new PutFlags[0]);
        c.put(bb(3), bb(4), new PutFlags[0]);
      }
      txn.commit();
    }
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
        c.first();
        final ByteBuffer key1 = c.key().duplicate();
        final ByteBuffer val1 = c.val().duplicate();

        c.last();
        final ByteBuffer key2 = c.key().duplicate();
        final ByteBuffer val2 = c.val().duplicate();

        assertThat(key1.getInt(0)).isEqualTo(1);
        assertThat(val1.getInt(0)).isEqualTo(2);

        assertThat(key2.getInt(0)).isEqualTo(3);
        assertThat(val2.getInt(0)).isEqualTo(4);
      }
    }
  }
}
