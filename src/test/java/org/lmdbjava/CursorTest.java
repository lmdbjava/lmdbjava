/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import static java.lang.Long.BYTES;
import static java.lang.Long.MIN_VALUE;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.PROXY_SAFE;
import static org.lmdbjava.ByteUnit.KIBIBYTES;
import org.lmdbjava.Cursor.ClosedException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;
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

  @Test
  public void cursorByteBufProxy() {
    final Env<ByteBuf> env = makeEnv(new ByteBufProxy());
    final Dbi<ByteBuf> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuf> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuf> c = db.openCursor(txn);
      c.put(nb(1), nb(2), MDB_NOOVERWRITE);
      c.put(nb(3), nb(4));
      c.put(nb(5), nb(6));
      c.put(nb(7), nb(8));

      // check MDB_SET operations
      final ByteBuf key3 = nb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final ByteBuf key6 = nb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
    }
  }

  @Test
  public void cursorByteBufferOptimal() {
    cursorByteBuffer(PROXY_OPTIMAL);
  }

  @Test
  public void cursorByteBufferSafe() {
    cursorByteBuffer(PROXY_SAFE);
  }

  @Test(expected = CommittedException.class)
  public void cursorCannotCloseIfTransactionCommitted() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      try (final Cursor<ByteBuffer> c = db.openCursor(txn);) {
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
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4));
      c.put(bb(5), bb(6));
      c.put(bb(7), bb(8));

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

  @Test
  public void cursorMutableDirectBuffer() {
    final Env<DirectBuffer> env = makeEnv(PROXY_DB);
    final Dbi<DirectBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<DirectBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<DirectBuffer> c = db.openCursor(txn);
      c.put(mdb(1), mdb(2), MDB_NOOVERWRITE);
      c.put(mdb(3), mdb(4));
      c.put(mdb(5), mdb(6));
      final DirectBuffer valForKey7 = c.reserve(mdb(7), BYTES);
      new UnsafeBuffer(valForKey7).putInt(0, 8);

      // check MDB_SET operations
      final MutableDirectBuffer key3 = mdb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final MutableDirectBuffer key6 = mdb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      final MutableDirectBuffer key999 = mdb(999);
      assertThat(c.get(key999, MDB_SET_KEY), is(false));

      // check MDB navigation operations
      assertThat(c.seek(MDB_LAST), is(true));
      final MutableDirectBuffer mdb1 = mdb(0);
      final MutableDirectBuffer mdb2 = mdb(0);
      mdb1.wrap(txn.key());
      mdb2.wrap(txn.val());

      assertThat(c.seek(MDB_PREV), is(true));
      final MutableDirectBuffer mdb3 = mdb(0);
      final MutableDirectBuffer mdb4 = mdb(0);
      mdb3.wrap(txn.key());
      mdb4.wrap(txn.val());

      assertThat(c.seek(MDB_NEXT), is(true));
      final MutableDirectBuffer mdb5 = mdb(0);
      final MutableDirectBuffer mdb6 = mdb(0);
      mdb5.wrap(txn.key());
      mdb6.wrap(txn.val());

      assertThat(c.seek(MDB_FIRST), is(true));
      final MutableDirectBuffer mdb7 = mdb(0);
      final MutableDirectBuffer mdb8 = mdb(0);
      mdb7.wrap(txn.key());
      mdb8.wrap(txn.val());

      // assert afterwards to ensure memory address from LMDB
      // are valid within same txn and across cursor movement
      // MDB_LAST
      assertThat(mdb1.getInt(0), is(7));
      assertThat(mdb2.getInt(0), is(8));

      // MDB_PREV
      assertThat(mdb3.getInt(0), is(5));
      assertThat(mdb4.getInt(0), is(6));

      // MDB_NEXT
      assertThat(mdb5.getInt(0), is(7));
      assertThat(mdb6.getInt(0), is(8));

      // MDB_FIRST
      assertThat(mdb7.getInt(0), is(1));
      assertThat(mdb8.getInt(0), is(2));
    }
  }

  @Test
  public void delete() {
    final Env<ByteBuffer> env = makeEnv(PROXY_OPTIMAL);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4));
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
    final ByteBuffer key = bb(5);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      assertNull(db.get(txn, key));
      final ByteBuffer val = c.reserve(key, BYTES * 2);
      assertNotNull(db.get(txn, key));
      val.putLong(MIN_VALUE).flip();
      txn.commit();
    }
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val.capacity(), is(BYTES * 2));
      assertThat(val.getLong(), is(MIN_VALUE));
    }
  }

  private void cursorByteBuffer(final BufferProxy<ByteBuffer> buffType) {
    final Env<ByteBuffer> env = makeEnv(buffType);
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(bb(1), bb(2), MDB_NOOVERWRITE);
      c.put(bb(3), bb(4));
      c.put(bb(5), bb(6));
      final ByteBuffer valForKey7 = c.reserve(bb(7), BYTES);
      valForKey7.putInt(8).flip();

      // check MDB_SET operations
      final ByteBuffer key3 = bb(3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(txn.key().getInt(0), is(3));
      assertThat(txn.val().getInt(0), is(4));
      final ByteBuffer key6 = bb(6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(txn.key().getInt(0), is(7));
      assertThat(txn.val().getInt(0), is(8));
      final ByteBuffer key999 = bb(999);
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

  private <T> Env<T> makeEnv(final BufferProxy<T> proxy) {
    try {
      final File path = tmp.newFile();
      final Env<T> env = create(proxy)
          .setMapSize(1_024, KIBIBYTES)
          .setMaxReaders(1)
          .setMaxDbs(1)
          .open(path, POSIX_MODE, MDB_NOSUBDIR);
      return env;
    } catch (IOException e) {
      throw new LmdbException("IO failure", e);
    }
  }

}
