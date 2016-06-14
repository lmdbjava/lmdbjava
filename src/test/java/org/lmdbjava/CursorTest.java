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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Cursor.ClosedException;
import org.lmdbjava.Cursor.CursorOpException;
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
      Val key = new Val(createBb());
      Val val = new Val(createBb());
      final Cursor cursor = db.openCursor(tx);
      cursor.close();
      cursor.position(key, val, MDB_FIRST);
    }
  }

  @Test
  public void cursorCanCountKeys() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createBb(1), createBb(2), MDB_APPENDDUP);
      assertThat(cursor.count(), is(1L));
      cursor.put(createBb(1), createBb(4), MDB_APPENDDUP);
      cursor.put(createBb(1), createBb(6), MDB_APPENDDUP);
      assertThat(cursor.count(), is(3L));
      cursor.put(createBb(2), createBb(1), MDB_APPENDDUP);
      cursor.put(createBb(2), createBb(2), MDB_APPENDDUP);
      assertThat(cursor.count(), is(2L));
    }
  }

  @Test(expected = CommittedException.class)
  public void cursorCannotCloseIfTransactionCommitted() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      try (Cursor cursor = db.openCursor(tx)) {
        cursor.put(createBb(1), createBb(2), MDB_APPENDDUP);
        assertThat(cursor.count(), is(1L));
        cursor.put(createBb(1), createBb(4), MDB_APPENDDUP);
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
      cursor.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      cursor.put(createBb(3), createBb(4));
      Val key = createVal();
      Val val = createVal();
      assertThat(cursor.position(key, val, MDB_FIRST), is(true));
      assertThat(key.getByteBuffer().getInt(), is(1));
      assertThat(val.getByteBuffer().getInt(), is(2));
      cursor.delete();
      assertThat(cursor.position(key, val, MDB_FIRST), is(true));
      assertThat(key.getByteBuffer().getInt(), is(3));
      assertThat(val.getByteBuffer().getInt(), is(4));
      cursor.delete();
      assertThat(cursor.position(key, val, MDB_FIRST), is(false));
    }
  }

  @Test
  public void get() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final Val key = new Val(createBb());
      final Val val = new Val(createBb());

      Cursor cursor = db.openCursor(tx);
      cursor.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      cursor.put(createBb(3), createBb(4));
      assertThat(cursor.position(key, val, MDB_FIRST), is(true));
      assertThat(key.getByteBuffer().getInt(), is(1));
      assertThat(val.getByteBuffer().getInt(), is(2));
      assertThat(cursor.position(key, val, MDB_NEXT), is(true));
      assertThat(key.getByteBuffer().getInt(), is(3));
      assertThat(val.getByteBuffer().getInt(), is(4));
      assertThat(cursor.position(key, val, MDB_PREV), is(true));
      assertThat(key.getByteBuffer().getInt(), is(1));
      assertThat(val.getByteBuffer().getInt(), is(2));
      assertThat(cursor.position(key, val, MDB_LAST), is(true));
      assertThat(key.getByteBuffer().getInt(), is(3));
      assertThat(val.getByteBuffer().getInt(), is(4));
    }
  }

  @Test(expected = CursorOpException.class)
  public void getMethodRejectsPositionOps() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
      final Cursor cursor = db.openCursor(tx);
      cursor.get(createVal(), createVal(), MDB_FIRST);
    }
  }

  @Test(expected = CursorOpException.class)
  public void positionMethodRejectsKey() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
      final Cursor cursor = db.openCursor(tx);
      cursor.position(createVal(), createVal(), MDB_SET_KEY);
    }
  }

  @Test
  public void renew() throws Exception {
    final Dbi db;
    try (final Txn tx = new Txn(env)) {
      db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      tx.commit();
    }

    final Cursor cursor;
    try (Txn tx = new Txn(env, MDB_RDONLY);) {
      cursor = db.openCursor(tx);
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

      final Cursor cursor = db.openCursor(tx);
      cursor.put(createBb(1), createBb(2));
      cursor.put(createBb(3), createBb(4));
      cursor.put(createBb(5), createBb(6));

      final Val key = new Val(createBb(1));
      final Val val = new Val(createBb());
      assertThat(cursor.get(key, val, MDB_SET), is(true));
      assertThat(key.getByteBuffer().getInt(), is(1));
      assertThat(val.getByteBuffer().getInt(), is(2));

      key.wrap(createBb(3));
      assertThat(cursor.get(key, val, MDB_SET_KEY), is(true));
      assertThat(key.getByteBuffer().getInt(), is(3));
      assertThat(val.getByteBuffer().getInt(), is(4));

      key.wrap(createBb(5));
      assertThat(cursor.get(key, val, MDB_SET_RANGE), is(true));
      assertThat(key.getByteBuffer().getInt(), is(5));
      assertThat(val.getByteBuffer().getInt(), is(6));

      key.wrap(createBb(0));
      assertThat(cursor.get(key, val, MDB_SET_RANGE), is(true));
      assertThat(key.getByteBuffer().getInt(), is(1));
      assertThat(val.getByteBuffer().getInt(), is(2));
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void testCursorRenewWriteTx() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);

      final Cursor cursor = db.openCursor(tx);
      cursor.renew(tx);
    }
  }
}
