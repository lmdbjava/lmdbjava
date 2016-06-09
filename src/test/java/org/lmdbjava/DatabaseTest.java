package org.lmdbjava;

import java.io.File;
import java.nio.ByteBuffer;
import static java.util.Collections.nCopies;
import java.util.Random;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Database.DbFullException;
import org.lmdbjava.Database.KeyNotFoundException;
import static org.lmdbjava.DatabaseFlags.MDB_CREATE;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import org.lmdbjava.LmdbNativeException.ConstantDerviedException;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.createBb;
import org.lmdbjava.Txn.CommittedException;

public class DatabaseTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Database db;
  private Env env;
  private Txn tx;

  @Before
  public void before() throws Exception {
    env = new Env();
    final File path = tmp.newFile();

    env.setMapSize(1_024 * 1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);

    tx = new Txn(env);
    db = new Database(tx, DB_1, MDB_CREATE);
  }

  @Test(expected = DbFullException.class)
  public void dbOpenMaxDatabases() throws Exception {
    new Database(tx, "another", MDB_CREATE);
  }

  @Test
  public void putAbortGet() throws Exception {
    Database db = new Database(tx, DB_1, MDB_CREATE);

    db.put(tx, createBb(5), createBb(5));
    tx.abort();

    try (Txn tx = new Txn(env)) {
      db.get(tx, createBb(5));
      fail("key does not exist");
    } catch (ConstantDerviedException e) {
      assertThat(e.getResultCode(), is(22));
    }
  }

  @Test
  public void putAndGetAndDeleteWithInternalTx() throws Exception {
    Database db = new Database(tx, DB_1, MDB_CREATE);
    tx.commit();
    db.put(createBb(5), createBb(5));
    ByteBuffer val = db.get(createBb(5));
    assertThat(val.getInt(), is(5));
    db.delete(createBb(5));
    try {
      db.get(createBb(5));
      fail("should have been deleted");
    } catch (KeyNotFoundException e) {
    }
  }

  @Test
  public void putCommitGet() throws Exception {
    Database db = new Database(tx, DB_1, MDB_CREATE);

    db.put(tx, createBb(5), createBb(5));
    tx.commit();

    try (Txn tx = new Txn(env)) {
      ByteBuffer result = db.get(tx, createBb(5));
      assertThat(result.getInt(), is(5));
    }
  }

  @Test
  public void putDelete() throws Exception {
    Database db = new Database(tx, DB_1, MDB_CREATE);

    db.put(tx, createBb(5), createBb(5));
    db.delete(tx, createBb(5));

    try {
      db.get(tx, createBb(5));
      fail("key does not exist");
    } catch (KeyNotFoundException e) {
    }
    tx.abort();
  }

  @Test
  public void testParallelWritesStress() throws Exception {
    tx.commit();
    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream()
        .forEach(ignored -> {
          Random random = new Random();
          for (int i = 0; i < 15_000; i++) {
            try {
              db.put(createBb(random.nextInt()), createBb(random.nextInt()));
            } catch (CommittedException | LmdbNativeException |
                     NotOpenException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }
}
