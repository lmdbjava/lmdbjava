package org.lmdbjava.core.lli;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.core.lli.exceptions.LmdbNativeException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.core.lli.DatabaseFlags.MDB_CREATE;
import static org.lmdbjava.core.lli.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.core.lli.TestUtils.DB_1;
import static org.lmdbjava.core.lli.TestUtils.POSIX_MODE;
import static org.lmdbjava.core.lli.TestUtils.createBb;

public class CursorTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env env;
  private Transaction tx;

  @Before
  public void before() throws Exception {
    env = new Env();
    final File path = tmp.newFile();

    final Set<EnvFlags> envFlags = new HashSet<>();
    envFlags.add(MDB_NOSUBDIR);

    env.setMapSize(1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, envFlags, POSIX_MODE);

    tx = env.txnBeginReadWrite();
  }

  @Test
  public void testCursorGet() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);
    db.put(tx, createBb(1), createBb(2));
    db.put(tx, createBb(3), createBb(4));

    Cursor cursor = db.openCursor(tx);

    ByteBuffer k = createBb();
    ByteBuffer v = createBb();

    cursor.get(k, v, CursorOp.MDB_FIRST);
    assertThat(k.getInt(), is(1));
    assertThat(v.getInt(), is(2));

    cursor.get(k, v, CursorOp.MDB_NEXT);
    assertThat(k.getInt(), is(3));
    assertThat(v.getInt(), is(4));

    cursor.get(k, v, CursorOp.MDB_PREV);
    assertThat(k.getInt(), is(1));
    assertThat(v.getInt(), is(2));

    cursor.get(k, v, CursorOp.MDB_LAST);
    assertThat(k.getInt(), is(3));
    assertThat(v.getInt(), is(4));

    tx.commit();
  }

  @Test
  public void testCursorSeek() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);
    db.put(tx, createBb(1), createBb(2));
    db.put(tx, createBb(3), createBb(4));
    db.put(tx, createBb(5), createBb(6));

    Cursor cursor = db.openCursor(tx);

    ByteBuffer k = createBb(1);
    ByteBuffer v = createBb();

    cursor.seekKey(k, v);
    assertThat(k.getInt(), is(1));
    assertThat(v.getInt(), is(2));

    k = createBb(3);
    cursor.seekKey(k, v);
    assertThat(k.getInt(), is(3));
    assertThat(v.getInt(), is(4));

    k = createBb(5);
    cursor.seekKey(k, v);
    assertThat(k.getInt(), is(5));
    assertThat(v.getInt(), is(6));

    tx.commit();
  }

  @Test(expected = IllegalArgumentException.class)
  public void closeCursor() throws LmdbNativeException, AlreadyCommittedException {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);
    Cursor cursor = db.openCursor(tx);
    cursor.close();
    ByteBuffer k = createBb(1);
    ByteBuffer v = createBb(1);
    cursor.get(k, v, CursorOp.MDB_FIRST);
  }
}
