/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import static java.lang.Integer.MAX_VALUE;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.PROXY_SAFE;
import org.lmdbjava.Cursor.ClosedException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;
import static org.lmdbjava.MutableDirectBufferProxy.PROXY_MDB;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_NEXT;
import static org.lmdbjava.SeekOp.MDB_PREV;
import static org.lmdbjava.TestUtils.*;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;

public class CursorTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected = ClosedException.class)
  public void closedCursorRejectsSubsequentGets() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.close();
      c.seek(MDB_FIRST);
    }
  }

  @Test
  public void count() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(createBb(1), createBb(2), MDB_APPENDDUP);
      assertThat(c.count(), is(1L));
      c.put(createBb(1), createBb(4), MDB_APPENDDUP);
      c.put(createBb(1), createBb(6), MDB_APPENDDUP);
      assertThat(c.count(), is(3L));
      c.put(createBb(2), createBb(1), MDB_APPENDDUP);
      c.put(createBb(2), createBb(2), MDB_APPENDDUP);
      assertThat(c.count(), is(2L));
    }
  }

  @Test
  public void cursorByteBufProxy() {
    final Env<ByteBuf> env = makeEnv(new ByteBufProxy());
    final Dbi<ByteBuf> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuf> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuf> c = db.openCursor(txn);
      c.put(allocateNb(1), allocateNb(2), MDB_NOOVERWRITE);
      c.put(allocateNb(3), allocateNb(4));
      c.put(allocateNb(5), allocateNb(6));
      c.put(allocateNb(7), allocateNb(8));

      // check MDB_SET operations
      final ByteBuf key3 = allocateNb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final ByteBuf key6 = allocateNb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      final ByteBuf key999 = allocateNb(999);
      assertThat(c.get(key999, MDB_SET_KEY), is(false));

      // check MDB navigation operations
      assertThat(c.seek(MDB_LAST), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_PREV), is(true));
      assertThat(txn.key().getInt(0), is(5));
      assertThat(txn.val().getInt(0), is(6));
      assertThat(c.seek(MDB_NEXT), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(txn.key().getInt(0), is(1));
      assertThat(txn.val().getInt(0), is(2));
    }
  }

  @Test
  public void cursorByteBufferOptimal() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      c.put(createBb(3), createBb(4));
      c.put(createBb(5), createBb(6));
      c.put(createBb(7), createBb(8));

      // check MDB_SET operations
      final ByteBuffer key3 = createBb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final ByteBuffer key6 = createBb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      final ByteBuffer key999 = createBb(999);
      assertThat(c.get(key999, MDB_SET_KEY), is(false));

      // check MDB navigation operations
      assertThat(c.seek(MDB_LAST), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_PREV), is(true));
      assertThat(txn.key().getInt(0), is(5));
      assertThat(txn.val().getInt(0), is(6));
      assertThat(c.seek(MDB_NEXT), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(txn.key().getInt(0), is(1));
      assertThat(txn.val().getInt(0), is(2));
    }
  }

  @Test
  public void cursorByteBufferSafe() {
    final Env<ByteBuffer> env = makeEnv(PROXY_SAFE);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      c.put(createBb(3), createBb(4));
      c.put(createBb(5), createBb(6));
      c.put(createBb(7), createBb(8));

      // check MDB_SET operations
      final ByteBuffer key3 = createBb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final ByteBuffer key6 = createBb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      final ByteBuffer key999 = createBb(999);
      assertThat(c.get(key999, MDB_SET_KEY), is(false));

      // check MDB navigation operations
      assertThat(c.seek(MDB_LAST), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_PREV), is(true));
      assertThat(txn.key().getInt(0), is(5));
      assertThat(txn.val().getInt(0), is(6));
      assertThat(c.seek(MDB_NEXT), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(txn.key().getInt(0), is(1));
      assertThat(txn.val().getInt(0), is(2));
    }
  }

  @Test(expected = CommittedException.class)
  public void cursorCannotCloseIfTransactionCommitted() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      try (final Cursor<ByteBuffer> c = db.openCursor(txn);) {
        c.put(createBb(1), createBb(2), MDB_APPENDDUP);
        assertThat(c.count(), is(1L));
        c.put(createBb(1), createBb(4), MDB_APPENDDUP);
        assertThat(c.count(), is(2L));
        txn.commit();
      }
    }
  }

  @Test
  public void cursorMutableDirectBuffer() {
    final Env<MutableDirectBuffer> env = makeEnv(PROXY_MDB);
    final Dbi<MutableDirectBuffer> db = env.openDbi(DB_1, MDB_CREATE,
                                                    MDB_DUPSORT);
    try (final Txn<MutableDirectBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<MutableDirectBuffer> c = db.openCursor(txn);
      c.put(createMdb(1), createMdb(2), MDB_NOOVERWRITE);
      c.put(createMdb(3), createMdb(4));
      c.put(createMdb(5), createMdb(6));
      c.put(createMdb(7), createMdb(8));

      // check MDB_SET operations
      final MutableDirectBuffer key3 = createMdb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final MutableDirectBuffer key6 = createMdb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      final MutableDirectBuffer key999 = createMdb(999);
      assertThat(c.get(key999, MDB_SET_KEY), is(false));

      // check MDB navigation operations
      assertThat(c.seek(MDB_LAST), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_PREV), is(true));
      assertThat(txn.key().getInt(0), is(5));
      assertThat(txn.val().getInt(0), is(6));
      assertThat(c.seek(MDB_NEXT), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(txn.key().getInt(0), is(1));
      assertThat(txn.val().getInt(0), is(2));
    }
  }

  @Test
  public void delete() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      c.put(createBb(3), createBb(4));
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(txn.key().getInt(), is(1));
      assertThat(txn.val().getInt(), is(2));
      c.delete();
      assertThat(c.seek(MDB_FIRST), is(true));
      assertThat(txn.key().getInt(), is(3));
      assertThat(txn.val().getInt(), is(4));
      c.delete();
      assertThat(c.seek(MDB_FIRST), is(false));
    }
  }

  @Test
  public void renewTxRo() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final Cursor<ByteBuffer> c;
    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      c = db.openCursor(txn);
      txn.commit();
    }

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      c.renew(txn);
      txn.commit();
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void renewTxRw() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(txn.isReadOnly(), is(false));

      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.renew(txn);
    }
  }

  @Test
  public void repeatedCloseCausesNotError() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.close();
      c.close();
    }
  }

  @Test
  public void reserve() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    final ByteBuffer key = createBb(5);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      assertNull(db.get(txn, key));
      final ByteBuffer val = c.reserve(key, Long.BYTES * 2);
      assertNotNull(db.get(txn, key));
      val.putLong(Long.MIN_VALUE).flip();
      txn.commit();
    }
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val.capacity(), is(Long.BYTES * 2));
      assertThat(val.getLong(), is(Long.MIN_VALUE));
    }
  }

  private <T> Env<T> makeEnv(final BufferProxy<T> proxy) {
    try {
      final File path = tmp.newFile();
      final Env<T> env = create(proxy)
        .setMapSize(1_024, ByteUnit.KIBIBYTES)
        .setMaxReaders(1)
        .setMaxDbs(1)
        .open(path, POSIX_MODE, MDB_NOSUBDIR);
      return env;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void cursorFirstLastNextPrev() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      c.put(createBb(3), createBb(4));
      c.put(createBb(5), createBb(6));
      c.put(createBb(7), createBb(8));

      assertThat(c.first(), is(true));
      assertThat(txn.key().getInt(0), is(1));
      assertThat(txn.val().getInt(0), is(2));

      assertThat(c.last(), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));

      assertThat(c.prev(), is(true));
      assertThat(txn.key().getInt(0), is(5));
      assertThat(txn.val().getInt(0), is(6));

      assertThat(c.first(), is(true));
      assertThat(c.next(), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
    }
  }
}
