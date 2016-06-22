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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Cursor.ClosedException;

import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.CursorOp.MDB_FIRST;
import static org.lmdbjava.CursorOp.MDB_LAST;
import static org.lmdbjava.CursorOp.MDB_NEXT;
import static org.lmdbjava.CursorOp.MDB_PREV;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.MutableDirectBufferProxy.FACTORY_MDB;
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
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE);
      final Cursor<ByteBuffer> c = db.openCursor(tx);
      final ByteBuffer key = allocateBb(db, 1);
      final ByteBuffer val = allocateBb(db, 1);
      c.close();
      c.get(key, MDB_FIRST);
    }
  }

  @Test
  public void count() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE, MDB_DUPSORT);
      final Cursor<ByteBuffer> c = db.openCursor(tx);
      c.put(allocateBb(db, 1), allocateBb(db, 2), MDB_APPENDDUP);
      assertThat(c.count(), is(1L));
      c.put(allocateBb(db, 1), allocateBb(db, 4), MDB_APPENDDUP);
      c.put(allocateBb(db, 1), allocateBb(db, 6), MDB_APPENDDUP);
      assertThat(c.count(), is(3L));
      c.put(allocateBb(db, 2), allocateBb(db, 1), MDB_APPENDDUP);
      c.put(allocateBb(db, 2), allocateBb(db, 2), MDB_APPENDDUP);
      assertThat(c.count(), is(2L));
    }
  }

  @Test(expected = CommittedException.class)
  public void cursorCannotCloseIfTransactionCommitted() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE, MDB_DUPSORT);

      try (final Cursor<ByteBuffer> c = db.openCursor(tx);) {
        c.put(allocateBb(db, 1), allocateBb(db, 2), MDB_APPENDDUP);
        assertThat(c.count(), is(1L));
        c.put(allocateBb(db, 1), allocateBb(db, 4), MDB_APPENDDUP);
        assertThat(c.count(), is(2L));
        tx.commit();
      }
    }
  }

  @Test
  public void delete() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE, MDB_DUPSORT);
      final Cursor<ByteBuffer> c = db.openCursor(tx);
      c.put(allocateBb(db, 1), allocateBb(db, 2), MDB_NOOVERWRITE);
      c.put(allocateBb(db, 3), allocateBb(db, 4));
      assertThat(c.get(null, MDB_FIRST), is(true));
      assertThat(c.key().getInt(), is(1));
      assertThat(c.val().getInt(), is(2));
      c.delete();
      assertThat(c.get(null, MDB_FIRST), is(true));
      assertThat(c.key().getInt(), is(3));
      assertThat(c.val().getInt(), is(4));
      c.delete();
      assertThat(c.get(null, MDB_FIRST), is(false));
    }
  }

  @Test
  public void getWithByteBufferOptimal() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE, MDB_DUPSORT);

      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(tx);
      c.put(allocateBb(db, 1), allocateBb(db, 2), MDB_NOOVERWRITE);
      c.put(allocateBb(db, 3), allocateBb(db, 4));
      c.put(allocateBb(db, 5), allocateBb(db, 6));
      c.put(allocateBb(db, 7), allocateBb(db, 8));

      // check MDB_SET operations
      final ByteBuffer key3 = allocateBb(db, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final ByteBuffer key6 = allocateBb(db, 6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));

      // check MDB navigation operations
      assertThat(c.get(null, MDB_LAST), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_PREV), is(true));
      assertThat(c.key().getInt(0), is(5));
      assertThat(c.val().getInt(0), is(6));
      assertThat(c.get(null, MDB_NEXT), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_FIRST), is(true));
      assertThat(c.key().getInt(0), is(1));
      assertThat(c.val().getInt(0), is(2));
    }
  }

  @Test
  public void getWithByteBufferSafe() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE, MDB_DUPSORT);

      // populate data
      final Cursor<ByteBuffer> c = db.openCursor(tx);
      c.put(allocateBb(db, 1), allocateBb(db, 2), MDB_NOOVERWRITE);
      c.put(allocateBb(db, 3), allocateBb(db, 4));
      c.put(allocateBb(db, 5), allocateBb(db, 6));
      c.put(allocateBb(db, 7), allocateBb(db, 8));

      // check MDB_SET operations
      final ByteBuffer key3 = allocateBb(db, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final ByteBuffer key6 = allocateBb(db, 6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));

      // check MDB navigation operations
      assertThat(c.get(null, MDB_LAST), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_PREV), is(true));
      assertThat(c.key().getInt(0), is(5));
      assertThat(c.val().getInt(0), is(6));
      assertThat(c.get(null, MDB_NEXT), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_FIRST), is(true));
      assertThat(c.key().getInt(0), is(1));
      assertThat(c.val().getInt(0), is(2));
    }
  }

  @Test
  public void getWithMutableByteBuffer() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<MutableDirectBuffer> db = new Dbi<>(tx, DB_1, FACTORY_MDB,
                                                    MDB_CREATE, MDB_DUPSORT);

      // populate data
      final Cursor<MutableDirectBuffer> c = db.openCursor(tx);
      c.put(allocateMdb(db, 1), allocateMdb(db, 2), MDB_NOOVERWRITE);
      c.put(allocateMdb(db, 3), allocateMdb(db, 4));
      c.put(allocateMdb(db, 5), allocateMdb(db, 6));
      c.put(allocateMdb(db, 7), allocateMdb(db, 8));

      // check MDB_SET operations
      final MutableDirectBuffer key3 = allocateMdb(db, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final MutableDirectBuffer key6 = allocateMdb(db, 6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));

      // check MDB navigation operations
      assertThat(c.get(null, MDB_LAST), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_PREV), is(true));
      assertThat(c.key().getInt(0), is(5));
      assertThat(c.val().getInt(0), is(6));
      assertThat(c.get(null, MDB_NEXT), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_FIRST), is(true));
      assertThat(c.key().getInt(0), is(1));
      assertThat(c.val().getInt(0), is(2));
    }
  }

  @Test
  public void renewTxRo() throws Exception {
    final Dbi<ByteBuffer> db;
    try (final Txn tx = new Txn(env)) {
      db = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE, MDB_DUPSORT);
      tx.commit();
    }

    final Cursor<ByteBuffer> c;
    try (final Txn tx = new Txn(env, MDB_RDONLY);) {
      c = db.openCursor(tx);
      tx.commit();
    }

    try (final Txn tx = new Txn(env, MDB_RDONLY);) {
      c.renew(tx);
      tx.commit();
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void renewTxRw() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE);

      final Cursor<ByteBuffer> c = db.openCursor(tx);
      c.renew(tx);
    }
  }

  @Test
  public void repeatedCloseCausesNotError() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi<ByteBuffer> db
          = new Dbi<>(tx, DB_1, PROXY_OPTIMAL, MDB_CREATE);
      final Cursor<ByteBuffer> c = db.openCursor(tx);
      c.close();
      c.close();
    }
  }
}
