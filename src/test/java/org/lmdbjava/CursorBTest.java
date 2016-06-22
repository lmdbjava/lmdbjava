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
import static org.lmdbjava.ByteBufferCursor.FACTORY_OPTIMAL;
import static org.lmdbjava.ByteBufferCursor.FACTORY_SAFE;
import org.lmdbjava.CursorB.ClosedException;
import static org.lmdbjava.CursorOp.MDB_FIRST;
import static org.lmdbjava.CursorOp.MDB_LAST;
import static org.lmdbjava.CursorOp.MDB_NEXT;
import static org.lmdbjava.CursorOp.MDB_PREV;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.MutableDirectBufferCursor.FACTORY_MDB;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.*;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

public class CursorBTest {

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
      final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_OPTIMAL);
      final ByteBuffer key = allocateBb(c, 1);
      final ByteBuffer val = allocateBb(c, 1);
      c.close();
      c.get(key, MDB_FIRST);
    }
  }

  @Test
  public void count() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_OPTIMAL);
      c.put(allocateBb(c, 1), allocateBb(c, 2), MDB_APPENDDUP);
      assertThat(c.count(), is(1L));
      c.put(allocateBb(c, 1), allocateBb(c, 4), MDB_APPENDDUP);
      c.put(allocateBb(c, 1), allocateBb(c, 6), MDB_APPENDDUP);
      assertThat(c.count(), is(3L));
      c.put(allocateBb(c, 2), allocateBb(c, 1), MDB_APPENDDUP);
      c.put(allocateBb(c, 2), allocateBb(c, 2), MDB_APPENDDUP);
      assertThat(c.count(), is(2L));
    }
  }

  @Test(expected = CommittedException.class)
  public void cursorCannotCloseIfTransactionCommitted() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      try (final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_OPTIMAL);) {
        c.put(allocateBb(c, 1), allocateBb(c, 2), MDB_APPENDDUP);
        assertThat(c.count(), is(1L));
        c.put(allocateBb(c, 1), allocateBb(c, 4), MDB_APPENDDUP);
        assertThat(c.count(), is(2L));
        tx.commit();
      }
    }
  }

  @Test
  public void delete() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_OPTIMAL);
      c.put(allocateBb(c, 1), allocateBb(c, 2), MDB_NOOVERWRITE);
      c.put(allocateBb(c, 3), allocateBb(c, 4));
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
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      // populate data
      final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_OPTIMAL);
      c.put(allocateBb(c, 1), allocateBb(c, 2), MDB_NOOVERWRITE);
      c.put(allocateBb(c, 3), allocateBb(c, 4));
      c.put(allocateBb(c, 5), allocateBb(c, 6));
      c.put(allocateBb(c, 7), allocateBb(c, 8));

      // check MDB_SET operations
      final ByteBuffer key3 = allocateBb(c, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final ByteBuffer key6 = allocateBb(c, 6);
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
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      // populate data
      final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_SAFE);
      c.put(allocateBb(c, 1), allocateBb(c, 2), MDB_NOOVERWRITE);
      c.put(allocateBb(c, 3), allocateBb(c, 4));
      c.put(allocateBb(c, 5), allocateBb(c, 6));
      c.put(allocateBb(c, 7), allocateBb(c, 8));

      // check MDB_SET operations
      final ByteBuffer key3 = allocateBb(c, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final ByteBuffer key6 = allocateBb(c, 6);
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
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      // populate data
      final CursorB<MutableDirectBuffer> c = db.openCursor(tx, FACTORY_MDB);
      c.put(allocateMdb(c, 1), allocateMdb(c, 2), MDB_NOOVERWRITE);
      c.put(allocateMdb(c, 3), allocateMdb(c, 4));
      c.put(allocateMdb(c, 5), allocateMdb(c, 6));
      c.put(allocateMdb(c, 7), allocateMdb(c, 8));

      // check MDB_SET operations
      final MutableDirectBuffer key3 = allocateMdb(c, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final MutableDirectBuffer key6 = allocateMdb(c, 6);
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
    final Dbi db;
    try (final Txn tx = new Txn(env)) {
      db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      tx.commit();
    }

    final CursorB<ByteBuffer> c;
    try (final Txn tx = new Txn(env, MDB_RDONLY);) {
      c = db.openCursor(tx, FACTORY_OPTIMAL);
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
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);

      final CursorB<ByteBuffer> c = db.openCursor(tx, FACTORY_OPTIMAL);
      c.renew(tx);
    }
  }

  @Test
  public void repeatedCloseCausesNotError() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
      final Cursor c = db.openCursor(tx);
      c.close();
      c.close();
    }
  }
}
