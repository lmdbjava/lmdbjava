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
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorB.ClosedException;
import static org.lmdbjava.CursorOp.MDB_FIRST;
import static org.lmdbjava.CursorOp.MDB_LAST;
import static org.lmdbjava.CursorOp.MDB_NEXT;
import static org.lmdbjava.CursorOp.MDB_PREV;
import static org.lmdbjava.CursorOp.MDB_SET;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;

import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;

import static org.lmdbjava.TestUtils.*;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;
import static org.lmdbjava.ByteBufferVals.forBuffer;
import static org.lmdbjava.MutableDirectBufferVal.forMdb;

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
      ValB key = createValBb(1);
      ValB val = createValBb(1);
      final CursorB cursor = db.openCursorB(tx);
      cursor.close();
      cursor.get(key, val, MDB_FIRST);
    }
  }

  @Test
  public void cursorCanCountKeys() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final CursorB cursor = db.openCursorB(tx);
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

      try (CursorB cursor = db.openCursorB(tx)) {
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
      final CursorB cursor = db.openCursorB(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      final ByteBuffer keyBb = createBb();
      final ByteBuffer valBb = createBb();
      ValB key = forBuffer(keyBb);
      ValB val = forBuffer(valBb);
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
  public void get() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final ByteBuffer keyBb = createBb();
      final ByteBuffer valBb = createBb();
      ValB key = forBuffer(keyBb);
      ValB val = forBuffer(valBb);

      CursorB cursor = db.openCursorB(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      assertThat(cursor.get(key, val, MDB_FIRST), is(true));
      assertThat(keyBb.getInt(), is(1));
      assertThat(valBb.getInt(), is(2));
      assertThat(cursor.get(key, val, MDB_NEXT), is(true));
      assertThat(keyBb.getInt(), is(3));
      assertThat(valBb.getInt(), is(4));
      assertThat(cursor.get(key, val, MDB_PREV), is(true));
      assertThat(keyBb.getInt(), is(1));
      assertThat(valBb.getInt(), is(2));
      assertThat(cursor.get(key, val, MDB_LAST), is(true));
      assertThat(keyBb.getInt(), is(3));
      assertThat(valBb.getInt(), is(4));
    }
  }

  @Test
  public void getAgrona() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final MutableDirectBuffer keyMdb = new UnsafeBuffer(createBb());
      final MutableDirectBuffer valMdb = new UnsafeBuffer(createBb());
      ValB key = forMdb(keyMdb);
      ValB val = forMdb(valMdb);

      CursorB cursor = db.openCursorB(tx);
      cursor.put(createValBb(1), createValBb(2), MDB_NOOVERWRITE);
      cursor.put(createValBb(3), createValBb(4));
      assertThat(cursor.get(key, val, MDB_FIRST), is(true));
      assertThat(keyMdb.getInt(0), is(1));
      assertThat(valMdb.getInt(0), is(2));
      assertThat(cursor.get(key, val, MDB_NEXT), is(true));
      assertThat(keyMdb.getInt(0), is(3));
      assertThat(valMdb.getInt(0), is(4));
      assertThat(cursor.get(key, val, MDB_PREV), is(true));
      assertThat(keyMdb.getInt(0), is(1));
      assertThat(valMdb.getInt(0), is(2));
      assertThat(cursor.get(key, val, MDB_LAST), is(true));
      assertThat(keyMdb.getInt(0), is(3));
      assertThat(valMdb.getInt(0), is(4));
    }
  }

  @Test
  public void renew() throws Exception {
    final Dbi db;
    try (final Txn tx = new Txn(env)) {
      db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      tx.commit();
    }

    final CursorB cursor;
    try (Txn tx = new Txn(env, MDB_RDONLY);) {
      cursor = db.openCursorB(tx);
      tx.commit();
    }

    try (Txn tx = new Txn(env, MDB_RDONLY);) {
      cursor.renew(tx);
      tx.commit();
    }
  }

  @Test
  public void set() throws Exception {
    try (final Txn tx = new Txn(env);) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      final CursorB cursor = db.openCursorB(tx);
      cursor.put(createValBb(1), createValBb(2));
      cursor.put(createValBb(3), createValBb(4));
      cursor.put(createValBb(5), createValBb(6));

      final ByteBuffer valBb = createBb();
      ByteBufferVal key = createValBb(1);
      ValB val = forBuffer(valBb);
      
      assertThat(cursor.get(key, val, MDB_SET), is(true));
      assertThat(key.buffer().getInt(), is(1));
      assertThat(valBb.getInt(), is(2));

      key.wrap(createBb(3));
      assertThat(cursor.get(key, val, MDB_SET_KEY), is(true));
      assertThat(key.buffer().getInt(), is(3));
      assertThat(valBb.getInt(), is(4));

      key.wrap(createBb(5));
      assertThat(cursor.get(key, val, MDB_SET_RANGE), is(true));
      assertThat(key.buffer().getInt(), is(5));
      assertThat(valBb.getInt(), is(6));

      key.wrap(createBb(0));
      assertThat(cursor.get(key, val, MDB_SET_RANGE), is(true));
      assertThat(key.buffer().getInt(), is(1));
      assertThat(valBb.getInt(), is(2));
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void testCursorRenewWriteTx() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);

      final CursorB cursor = db.openCursorB(tx);
      cursor.renew(tx);
    }
  }
}
