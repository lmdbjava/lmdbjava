package org.lmdbjava;

import java.io.File;
import java.nio.ByteBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Cursor.ClosedException;
import static org.lmdbjava.CursorOp.MDB_FIRST;
import static org.lmdbjava.CursorOp.MDB_LAST;
import static org.lmdbjava.CursorOp.MDB_NEXT;
import static org.lmdbjava.CursorOp.MDB_PREV;
import static org.lmdbjava.CursorOp.MDB_SET;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import org.lmdbjava.Dbi.KeyNotFoundException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.createBb;
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
      final Cursor cursor = db.openCursor(tx);
      cursor.close();
      final ByteBuffer k = createBb(1);
      final ByteBuffer v = createBb(1);
      cursor.get(k, v, MDB_FIRST);
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

  @Test(expected = KeyNotFoundException.class)
  public void testCursorDelete() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      final Cursor cursor = db.openCursor(tx);
      cursor.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      cursor.put(createBb(3), createBb(4));
      final ByteBuffer k = createBb(1);
      final ByteBuffer v = createBb();
      cursor.get(k, v, MDB_FIRST);
      assertThat(k.getInt(), is(1));
      assertThat(v.getInt(), is(2));
      cursor.delete();
      cursor.get(k, v, MDB_FIRST);
      assertThat(k.getInt(), is(3));
      assertThat(v.getInt(), is(4));
      cursor.delete();
      cursor.get(k, v, MDB_FIRST);
    }
  }

  @Test
  public void testCursorGet() throws Exception {
    try (final Txn tx = new Txn(env)) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);
      Cursor cursor = db.openCursor(tx);
      cursor.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
      cursor.put(createBb(3), createBb(4));
      ByteBuffer k = createBb();
      ByteBuffer v = createBb();
      cursor.get(k, v, MDB_FIRST);
      assertThat(k.getInt(), is(1));
      assertThat(v.getInt(), is(2));
      cursor.get(k, v, MDB_NEXT);
      assertThat(k.getInt(), is(3));
      assertThat(v.getInt(), is(4));
      cursor.get(k, v, MDB_PREV);
      assertThat(k.getInt(), is(1));
      assertThat(v.getInt(), is(2));
      cursor.get(k, v, MDB_LAST);
      assertThat(k.getInt(), is(3));
      assertThat(v.getInt(), is(4));
    }
  }

  @Test
  public void testCursorRenew() throws Exception {
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

  @Test(expected = ReadOnlyRequiredException.class)
  public void testCursorRenewWriteTx() throws Exception {
    try (final Txn tx = new Txn(env);) {
      assertThat(tx.isReadOnly(), is(false));
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
      final Cursor cursor = db.openCursor(tx);
      cursor.renew(tx);
    }
  }

  @Test
  public void testCursorSet() throws Exception {
    try (final Txn tx = new Txn(env);) {
      final Dbi db = new Dbi(tx, DB_1, MDB_CREATE, MDB_DUPSORT);

      final Cursor cursor = db.openCursor(tx);
      cursor.put(createBb(1), createBb(2));
      cursor.put(createBb(3), createBb(4));
      cursor.put(createBb(5), createBb(6));

      final ByteBuffer v = createBb();

      final ByteBuffer k1 = createBb(1);
      cursor.get(k1, v, MDB_SET);
      assertThat(k1.getInt(), is(1));
      assertThat(v.getInt(), is(2));

      final ByteBuffer k3 = createBb(3);
      cursor.get(k3, v, MDB_SET_KEY);
      assertThat(k3.getInt(), is(3));
      assertThat(v.getInt(), is(4));

      final ByteBuffer k5 = createBb(5);
      cursor.get(k5, v, MDB_SET_RANGE);
      assertThat(k5.getInt(), is(5));
      assertThat(v.getInt(), is(6));

      final ByteBuffer k0 = createBb(0);
      cursor.get(k0, v, MDB_SET_RANGE);
      assertThat(k0.getInt(), is(1));
      assertThat(v.getInt(), is(2));
    }
  }
}
