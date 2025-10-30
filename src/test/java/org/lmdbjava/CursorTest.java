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

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import static java.lang.Long.BYTES;
import static java.lang.Long.MIN_VALUE;
import static java.nio.ByteBuffer.allocateDirect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Cursor.ClosedException;
import org.lmdbjava.Txn.NotReadyException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;

/** Test {@link Cursor}. */
public final class CursorTest {

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private Env<ByteBuffer> env;

  @After
  public void after() {
    env.close();
  }

  @Before
  public void before() throws IOException {
    try {
      final File path = tmp.newFile();
      env =
          create(ByteBufferProxy.INSTANCE)
              .setMapSize(KIBIBYTES.toBytes(1_024))
              .setMaxReaders(1)
              .setMaxDbs(1)
              .open(path, POSIX_MODE, MDB_NOSUBDIR);
    } catch (final IOException e) {
      throw new LmdbException("IO failure", e);
    }
  }

  @Test(expected = ClosedException.class)
  public void closedCursorRejectsSubsequentGets() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.close();
      c.seek(MDB_FIRST);
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsSeekFirstCall() {
    doEnvClosedTest(null, c -> c.seek(MDB_FIRST));
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsSeekLastCall() {
    doEnvClosedTest(null, c -> c.seek(MDB_LAST));
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsSeekNextCall() {
    doEnvClosedTest(null, c -> c.seek(MDB_NEXT));
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsCloseCall() {
    doEnvClosedTest(null, Cursor::close);
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsFirstCall() {
    doEnvClosedTest(null, Cursor::first);
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsLastCall() {
    doEnvClosedTest(null, Cursor::last);
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsPrevCall() {
    doEnvClosedTest(
        c -> {
          c.first();
          assertThat(c.key().getInt(), is(1));
          assertThat(c.val().getInt(), is(10));
          c.next();
        },
        Cursor::prev);
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void closedEnvRejectsDeleteCall() {
    doEnvClosedTest(
        c -> {
          c.first();
          assertThat(c.key().getInt(), is(1));
          assertThat(c.val().getInt(), is(10));
        },
        Cursor::delete);
  }

  @Test
  public void count() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), MDB_APPENDDUP);
      assertThat(c.count(), is(1L));
      c.put(bb(1), bb(4), MDB_APPENDDUP);
      c.put(bb(1), bb(6), MDB_APPENDDUP);
      assertThat(c.count(), is(3L));
      c.put(bb(2), bb(1), MDB_APPENDDUP);
      c.put(bb(2), bb(2), MDB_APPENDDUP);
      assertThat(c.count(), is(2L));
    }
  }

  @Test(expected = NotReadyException.class)
  public void cursorCannotCloseIfTransactionCommitted() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (Cursor<ByteBuffer> c = db.openCursor(txn); ) {
        c.put(bb(1), bb(2), MDB_APPENDDUP);
        assertThat(c.count(), is(1L));
        c.put(bb(1), bb(4), MDB_APPENDDUP);
        assertThat(c.count(), is(2L));
        txn.commit();
      }
    }
  }

  @Test
  public void cursorFirstLastNextPrev() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4));
      c.put(bb(5), bb(6));
      c.put(bb(7), bb(8));

      assertThat(c.first(), is(true));
      assertThat(c.key().getInt(0), is(1));
      assertThat(c.val().getInt(0), is(2));

      assertThat(c.last(), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));

      assertThat(c.prev(), is(true));
      assertThat(c.key().getInt(0), is(5));
      assertThat(c.val().getInt(0), is(6));

      assertThat(c.first(), is(true));
      assertThat(c.next(), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
    }
  }

  @Test
  public void delete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4));
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(c.key().getInt(), is(1));
      assertThat(c.val().getInt(), is(2));
      c.delete();
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(c.key().getInt(), is(3));
      assertThat(c.val().getInt(), is(4));
      c.delete();
      assertThat(c.seek(MDB_FIRST), is(false));
    }
  }

  @Test
  public void getKeyVal() {
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
      assertThat(c.get(bb(1), bb(2), MDB_GET_BOTH), is(true));
      assertThat(c.count(), is(3L));
      assertThat(c.get(bb(1), bb(3), MDB_GET_BOTH), is(false));
      assertThat(c.get(bb(2), bb(1), MDB_GET_BOTH), is(true));
      assertThat(c.count(), is(4L));
      assertThat(c.get(bb(2), bb(0), MDB_GET_BOTH), is(false));
    }
  }

  @Test
  public void putMultiple() {
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
      assertThat(c.count(), is((long) elemCount));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void putMultipleWithoutMdbMultipleFlag() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      c.putMultiple(bb(100), bb(1), 1);
    }
  }

  @Test
  public void renewTxRo() {
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

  @Test(expected = ReadOnlyRequiredException.class)
  public void renewTxRw() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(txn.isReadOnly(), is(false));

      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
        c.renew(txn);
      }
    }
  }

  @Test
  public void repeatedCloseCausesNotError() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.close();
      c.close();
    }
  }

  @Test
  public void reserve() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    final ByteBuffer key = bb(5);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertNull(db.get(txn, key));
      try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
        final ByteBuffer val = c.reserve(key, BYTES * 2);
        assertNotNull(db.get(txn, key));
        val.putLong(MIN_VALUE).flip();
      }
      txn.commit();
    }
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val.capacity(), is(BYTES * 2));
      assertThat(val.getLong(), is(MIN_VALUE));
    }
  }

  @Test
  public void returnValueForNoDupData() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      // ok
      assertThat(c.put(bb(5), bb(6), MDB_NODUPDATA), is(true));
      assertThat(c.put(bb(5), bb(7), MDB_NODUPDATA), is(true));
      assertThat(c.put(bb(5), bb(6), MDB_NODUPDATA), is(false));
    }
  }

  @Test
  public void returnValueForNoOverwrite() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite();
        Cursor<ByteBuffer> c = db.openCursor(txn)) {
      // ok
      assertThat(c.put(bb(5), bb(6), MDB_NOOVERWRITE), is(true));
      // fails, but gets exist val
      assertThat(c.put(bb(5), bb(8), MDB_NOOVERWRITE), is(false));
      assertThat(c.val().getInt(0), is(6));
    }
  }

  @Test
  public void testCursorByteBufferDuplicate() {
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

        assertThat(key1.getInt(0), is(1));
        assertThat(val1.getInt(0), is(2));

        assertThat(key2.getInt(0), is(3));
        assertThat(val2.getInt(0), is(4));
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
