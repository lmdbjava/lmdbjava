package org.lmdbjava;

import java.io.File;
import java.nio.ByteBuffer;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CursorOp.MDB_FIRST;
import static org.lmdbjava.CursorOp.MDB_LAST;
import static org.lmdbjava.CursorOp.MDB_NEXT;
import static org.lmdbjava.CursorOp.MDB_PREV;
import static org.lmdbjava.CursorOp.MDB_SET;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import static org.lmdbjava.CursorOp.MDB_SET_RANGE;
import static org.lmdbjava.DatabaseFlags.MDB_CREATE;
import static org.lmdbjava.DatabaseFlags.MDB_DUPSORT;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.createBb;
import static org.lmdbjava.TransactionFlags.MDB_RDONLY;

public class CursorTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Database db;
  private Env env;
  private Transaction tx;

  @Before
  public void before() throws Exception {
    env = new Env();
    final File path = tmp.newFile();
    env.setMapSize(1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    tx = new Transaction(env, null);
    db = tx.databaseOpen(DB_1, MDB_CREATE, MDB_DUPSORT);
  }

  @Test(expected = IllegalArgumentException.class)
  public void closeCursor() throws LmdbNativeException,
                                   AlreadyCommittedException {
    Database db = tx.databaseOpen(DB_1, MDB_CREATE);
    Cursor cursor = db.openCursor(tx);
    cursor.close();
    ByteBuffer k = createBb(1);
    ByteBuffer v = createBb(1);
    cursor.get(k, v, MDB_FIRST);
  }

  @Test
  public void testCursorCount() throws Exception {
    Cursor cursor = db.openCursor(tx);

    cursor.put(createBb(1), createBb(2), MDB_APPENDDUP);
    assertThat(cursor.count(), is(1L));

    cursor.put(createBb(1), createBb(4), MDB_APPENDDUP);
    assertThat(cursor.count(), is(2L));
    tx.commit();
  }

  @Test
  public void testCursorDelete() throws Exception {
    Cursor cursor = db.openCursor(tx);
    cursor.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
    cursor.put(createBb(3), createBb(4));
    ByteBuffer k = createBb(1);
    ByteBuffer v = createBb();
    cursor.get(k, v, MDB_FIRST);
    assertThat(k.getInt(), is(1));
    assertThat(v.getInt(), is(2));
    cursor.delete();
    cursor.get(k, v, MDB_FIRST);
    assertThat(k.getInt(), is(3));
    assertThat(v.getInt(), is(4));
    cursor.delete();
    try {
      cursor.get(k, v, MDB_FIRST);
      fail("should fail");
    } catch (NotFoundException e) {
    }
    tx.commit();
  }

  @Test
  public void testCursorGet() throws Exception {
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
    tx.commit();
  }

  @Test
  public void testCursorRenew() throws Exception {
    tx.commit();
    tx = new Transaction(env, null, MDB_RDONLY);
    Cursor cursor = db.openCursor(tx);
    tx.commit();
    tx = new Transaction(env, null, MDB_RDONLY);
    cursor.renew(tx);
    tx.commit();
  }

  @Test
  public void testCursorRenewWriteTx() throws Exception {
    Cursor cursor = db.openCursor(tx);
    try {
      cursor.renew(tx);
      fail("should fail");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("cannot renew write transactions"));
    }
    tx.commit();
  }

  @Test
  public void testCursorSet() throws Exception {
    Cursor cursor = db.openCursor(tx);
    cursor.put(createBb(1), createBb(2));
    cursor.put(createBb(3), createBb(4));
    cursor.put(createBb(5), createBb(6));

    ByteBuffer k = createBb(1);
    ByteBuffer v = createBb();

    cursor.get(k, v, MDB_SET);
    assertThat(k.getInt(), is(1));
    assertThat(v.getInt(), is(2));

    k = createBb(3);
    cursor.get(k, v, MDB_SET_KEY);
    assertThat(k.getInt(), is(3));
    assertThat(v.getInt(), is(4));

    k = createBb(5);
    cursor.get(k, v, MDB_SET_RANGE);
    assertThat(k.getInt(), is(5));
    assertThat(v.getInt(), is(6));

    k = createBb(0);
    cursor.get(k, v, MDB_SET_RANGE);
    assertThat(k.getInt(), is(1));
    assertThat(v.getInt(), is(2));

    tx.commit();
  }

}
