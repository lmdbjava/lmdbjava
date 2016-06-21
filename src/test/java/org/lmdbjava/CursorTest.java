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

import java.io.File;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.ByteBufferVal.forBuffer;
import org.lmdbjava.Cursor.ClosedException;
import static org.lmdbjava.CursorOp.MDB_FIRST;
import static org.lmdbjava.CursorOp.MDB_LAST;
import static org.lmdbjava.CursorOp.MDB_NEXT;
import static org.lmdbjava.CursorOp.MDB_PREV;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.MutableDirectBufferVal.forMdb;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.*;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

public class CursorTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env env;

  @Before
  public void before() throws Exception {
    env = new Env();
    final File path = tmp.newFile();
    env.setMapSize(1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
  }

  @Test(expected = ClosedException.class)
  public void closedCursorRejectsSubsequentGets() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
      final Val key = createValBb(1);
      final Val val = createValBb(1);
      final Cursor cursor = db.openCursor(tx);
      cursor.close();
      cursor.get(key, val, MDB_FIRST);
    }
  }

  @Test
  public void count() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_APPENDDUP);
      assertThat(cursor.count(), is(1L));
      cursor.put(createValBb(1), createValBb(4), MDB_APPENDDUP);
      cursor.put(createValBb(1), createValBb(6), MDB_APPENDDUP);
      assertThat(cursor.count(), is(3L));
      cursor.put(createValBb(2), createValBb(1), MDB_APPENDDUP);
      cursor.put(createValBb(2), createValBb(2), MDB_APPENDDUP);
      assertThat(cursor.count(), is(2L));
    }
  }

  @Test(expected = CommittedException.class)
  public void cursorCannotCloseIfTransactionCommitted() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      try (final Cursor cursor = db.openCursor(tx)) {
        cursor.put(createValBb(1), createValBb(2), MDB_APPENDDUP);
        assertThat(cursor.count(), is(1L));
        cursor.put(createValBb(1), createValBb(4), MDB_APPENDDUP);
        assertThat(cursor.count(), is(2L));
        tx.commit();
      }
    }
  }

  @Test
  public void delete() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      final ByteBuffer keyBb = createBb();
      final ByteBuffer valBb = createBb();
      final Val key = forBuffer(keyBb);
      final Val val = forBuffer(valBb);
      assertThat(cursor.get(key, val, MDB_FIRST), is(true));
      assertThat(keyBb.getInt(), is(1));
      assertThat(valBb.getInt(), is(2));
      cursor.delete();
      assertThat(cursor.get(key, val, MDB_FIRST), is(true));
      assertThat(keyBb.getInt(), is(3));
      assertThat(valBb.getInt(), is(4));
      cursor.delete();
      assertThat(cursor.get(key, val, MDB_FIRST), is(false));
    }
  }

  @Test
  public void getWithByteBufferDefault() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      final ByteBuffer kb = createBb();
      final ByteBuffer vb = createBb();
      final ByteBufferVal kv = forBuffer(kb, true, false);
      assertThat(kv.buffer(), is(kb));
      final Val vv = forBuffer(vb, true, false);

      // populate data
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      cursor.put(createValBb(5), createValBb(6));
      cursor.put(createValBb(7), createValBb(8));

      // check MDB_SET operations
      kb.putInt(0, 3);
      assertThat(cursor.get(kv, vv, MDB_SET_KEY), is(true));
      assertThat(kb.getInt(0), is(3));
      assertThat(vb.getInt(0), is(4));
      final long key3Addr = kv.dataAddress();
      kv.wrap(createBb(6));
      final ByteBuffer kb2 = kv.buffer();
      assertThat(cursor.get(kv, vv, MDB_SET_RANGE), is(true));
      assertThat(kb2.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      final long key7Addr = kv.dataAddress();
      assertThat(key7Addr, not(key3Addr));

      // check MDB navigation operations
      assertThat(cursor.get(kv, vv, MDB_LAST), is(true));
      assertThat(kb2.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      assertThat(cursor.get(kv, vv, MDB_PREV), is(true));
      assertThat(kb2.getInt(0), is(5));
      assertThat(vb.getInt(0), is(6));
      assertThat(cursor.get(kv, vv, MDB_NEXT), is(true));
      assertThat(kb2.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      assertThat(cursor.get(kv, vv, MDB_FIRST), is(true));
      assertThat(kb2.getInt(0), is(1));
      assertThat(vb.getInt(0), is(2));
    }
  }

  @Test
  public void getWithByteBufferSafe() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      final ByteBuffer kb = createBb();
      final ByteBuffer vb = createBb();
      final ByteBufferVal kv = forBuffer(kb, true, true);
      assertThat(kv.buffer(), is(kb));
      final Val vv = forBuffer(vb, true, true);

      // populate data
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      cursor.put(createValBb(5), createValBb(6));
      cursor.put(createValBb(7), createValBb(8));

      // check MDB_SET operations
      kb.putInt(0, 3);
      assertThat(cursor.get(kv, vv, MDB_SET_KEY), is(true));
      assertThat(kb.getInt(0), is(3));
      assertThat(vb.getInt(0), is(4));
      final long key3Addr = kv.dataAddress();
      kv.wrap(createBb(6));
      final ByteBuffer kb2 = kv.buffer();
      assertThat(cursor.get(kv, vv, MDB_SET_RANGE), is(true));
      assertThat(kb2.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      final long key7Addr = kv.dataAddress();
      assertThat(key7Addr, not(key3Addr));

      // check MDB navigation operations
      assertThat(cursor.get(kv, vv, MDB_LAST), is(true));
      assertThat(kb2.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      assertThat(cursor.get(kv, vv, MDB_PREV), is(true));
      assertThat(kb2.getInt(0), is(5));
      assertThat(vb.getInt(0), is(6));
      assertThat(cursor.get(kv, vv, MDB_NEXT), is(true));
      assertThat(kb2.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      assertThat(cursor.get(kv, vv, MDB_FIRST), is(true));
      assertThat(kb2.getInt(0), is(1));
      assertThat(vb.getInt(0), is(2));
    }
  }

  @Test
  public void getWithMutableDirectBuffer() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      final MutableDirectBuffer kb = new UnsafeBuffer(createBb());
      final MutableDirectBuffer vb = new UnsafeBuffer(createBb());
      final MutableDirectBufferVal kv = forMdb(kb);
      assertThat(kv.buffer(), is(kb));
      final Val vv = forMdb(vb, true);

      // populate data
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      cursor.put(createValBb(5), createValBb(6));
      cursor.put(createValBb(7), createValBb(8));

      // check MDB_SET operations
      kb.putInt(0, 3);
      assertThat(cursor.get(kv, vv, MDB_SET_KEY), is(true));
      assertThat(kb.getInt(0), is(3));
      assertThat(vb.getInt(0), is(4));
      final long key3Addr = kv.dataAddress();
      kb.wrap(createBb(6));
      assertThat(cursor.get(kv, vv, MDB_SET_RANGE), is(true));
      assertThat(kb.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      final long key7Addr = kv.dataAddress();
      assertThat(key7Addr, not(key3Addr));

      // check MDB navigation operations
      assertThat(cursor.get(kv, vv, MDB_LAST), is(true));
      assertThat(kb.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      assertThat(cursor.get(kv, vv, MDB_PREV), is(true));
      assertThat(kb.getInt(0), is(5));
      assertThat(vb.getInt(0), is(6));
      assertThat(cursor.get(kv, vv, MDB_NEXT), is(true));
      assertThat(kb.getInt(0), is(7));
      assertThat(vb.getInt(0), is(8));
      assertThat(cursor.get(kv, vv, MDB_FIRST), is(true));
      assertThat(kb.getInt(0), is(1));
      assertThat(vb.getInt(0), is(2));
    }
  }

  @Test
  public void renewTxRo() throws Exception {
    final Dbi db;
    try (final Txn tx = new Txn(env)) {
      db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      tx.commit();
    }

    final Cursor cursor;
    try (final Txn tx = new Txn(env, MDB_RDONLY);) {
      cursor = db.openCursor(tx);
      tx.commit();
    }

    try (final Txn tx = new Txn(env, MDB_RDONLY);) {
      cursor.renew(tx);
      tx.commit();
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void renewTxRw() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);

      final Cursor cursor = db.openCursor(tx);
      cursor.renew(tx);
    }
  }

  @Test
  public void repeatedCloseCausesNotError() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
      final Cursor cursor = db.openCursor(tx);
      cursor.close();
      cursor.close();
    }
  }
}
