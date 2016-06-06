package org.lmdbjava.core.lli;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.core.lli.exceptions.ConstantDerviedException;
import org.lmdbjava.core.lli.exceptions.NotFoundException;

import static org.lmdbjava.core.lli.DatabaseFlags.MDB_CREATE;
import static org.lmdbjava.core.lli.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.core.lli.TestUtils.DB_1;
import static org.lmdbjava.core.lli.TestUtils.POSIX_MODE;

public class DatabaseTest {

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
  public void dbOpen() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);
    assertThat(db.getName(), is(DB_1));
  }

  @Test
  public void putCommitGet() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);

    db.put(tx, createBb(5), createBb(5));
    tx.commit();

    tx = env.txnBeginReadWrite();

    ByteBuffer result = db.get(tx, createBb(5));
    assertThat(result.getInt(0), is(5));
    tx.abort();
  }

  @Test
  public void putAbortGet() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);

    db.put(tx, createBb(5), createBb(5));
    tx.abort();

    tx = env.txnBeginReadWrite();
    try {
      db.get(tx, createBb(5));
      fail("key does not exist");
    } catch (ConstantDerviedException e) {
      assertThat(e.getResultCode(), is(22));
    }
    tx.abort();
  }

  @Test
  public void putDelete() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);

    db.put(tx, createBb(5), createBb(5));
    db.delete(tx, createBb(5));

    try {
      db.get(tx, createBb(5));
      fail("key does not exist");
    } catch (NotFoundException e) {
    }
    tx.abort();
  }

  private ByteBuffer createBb(int value) {
    ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(value).flip();
    return bb;
  }

}
