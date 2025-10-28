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

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static java.lang.Long.BYTES;
import static java.lang.Long.MIN_VALUE;
import static java.nio.ByteBuffer.allocateDirect;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
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
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_NEXT;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Cursor.ClosedException;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Txn.NotReadyException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;

/** Test {@link Cursor}. */
public final class CursorTest {

  private Path file;
  private Env<ByteBuffer> env;

  @BeforeEach
  void beforeEach() {
    file = FileUtil.createTempFile();
    env =
        create(PROXY_OPTIMAL)
            .setMapSize(MEBIBYTES.toBytes(1))
            .setMaxReaders(1)
            .setMaxDbs(1)
            .open(file.toFile(), POSIX_MODE, MDB_NOSUBDIR);
  }

  @AfterEach
  void afterEach() {
    env.close();
    FileUtil.delete(file);
  }

  @Test
  void closedCursorRejectsSubsequentGets() {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
              try (Txn<ByteBuffer> txn = env.txnWrite()) {
                final Cursor<ByteBuffer> c = db.openCursor(txn);
                c.close();
                c.seek(MDB_FIRST);
              }
            })
        .isInstanceOf(ClosedException.class);
  }

  @Test
  void closedEnvRejectsSeekFirstCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, c -> c.seek(MDB_FIRST));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsSeekLastCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, c -> c.seek(MDB_LAST));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsSeekNextCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, c -> c.seek(MDB_NEXT));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsCloseCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Cursor::close);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsFirstCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Cursor::first);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsLastCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Cursor::last);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsPrevCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(
                  c -> {
                    c.first();
                    assertThat(c.key().getInt()).isEqualTo(1);
                    assertThat(c.val().getInt()).isEqualTo(10);
                    c.next();
                  },
                  Cursor::prev);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsDeleteCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(
                  c -> {
                    c.first();
                    assertThat(c.key().getInt()).isEqualTo(1);
                    assertThat(c.val().getInt()).isEqualTo(10);
                  },
                  Cursor::delete);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void count() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), MDB_APPENDDUP);
      assertThat(c.count()).isEqualTo(1L);
      c.put(bb(1), bb(4), MDB_APPENDDUP);
      c.put(bb(1), bb(6), MDB_APPENDDUP);
      assertThat(c.count()).isEqualTo(3L);
      c.put(bb(2), bb(1), MDB_APPENDDUP);
      c.put(bb(2), bb(2), MDB_APPENDDUP);
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
                  c.put(bb(1), bb(2), MDB_APPENDDUP);
                  assertThat(c.count()).isEqualTo(1L);
                  c.put(bb(1), bb(4), MDB_APPENDDUP);
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
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4));
      c.put(bb(5), bb(6));
      c.put(bb(7), bb(8));

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
      c.put(bb(3), bb(4));
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
      c.put(bb(1), bb(2), MDB_APPENDDUP);
      c.put(bb(1), bb(4), MDB_APPENDDUP);
      c.put(bb(1), bb(6), MDB_APPENDDUP);
      c.put(bb(2), bb(1), MDB_APPENDDUP);
      c.put(bb(2), bb(2), MDB_APPENDDUP);
      c.put(bb(2), bb(3), MDB_APPENDDUP);
      c.put(bb(2), bb(4), MDB_APPENDDUP);
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
      c.putMultiple(k, values, elemCount, MDB_MULTIPLE);
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
                c.putMultiple(bb(100), bb(1), 1);
              }
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void renewTxRo() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final Cursor<ByteBuffer> c;
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      c = db.openCursor(txn);
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      c.renew(txn);
      txn.commit();
    }

    c.close();
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
        final ByteBuffer val = c.reserve(key, BYTES * 2);
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
      assertThat(c.put(bb(5), bb(6), MDB_NODUPDATA)).isTrue();
      assertThat(c.put(bb(5), bb(7), MDB_NODUPDATA)).isTrue();
      assertThat(c.put(bb(5), bb(6), MDB_NODUPDATA)).isFalse();
    }
  }

  @Test
  void returnValueForNoOverwrite() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      // ok
      assertThat(c.put(bb(5), bb(6), MDB_NOOVERWRITE)).isTrue();
      // fails, but gets exist val
      assertThat(c.put(bb(5), bb(8), MDB_NOOVERWRITE)).isFalse();
      assertThat(c.val().getInt(0)).isEqualTo(6);
    }
  }

  @Test
  void testCursorByteBufferDuplicate() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
        c.put(bb(1), bb(2));
        c.put(bb(3), bb(4));
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

  private void doEnvClosedTest(
      final Consumer<Cursor<ByteBuffer>> workBeforeEnvClosed,
      final Consumer<Cursor<ByteBuffer>> workAfterEnvClose) {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    db.put(bb(1), bb(10));
    db.put(bb(2), bb(20));
    db.put(bb(2), bb(30));
    db.put(bb(4), bb(40));

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {

        if (workBeforeEnvClosed != null) {
          workBeforeEnvClosed.accept(c);
        }

        env.close();

        if (workAfterEnvClose != null) {
          workAfterEnvClose.accept(c);
        }
      }
    }
  }
}
