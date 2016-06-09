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
import org.lmdbjava.Dbi.DbFullException;
import org.lmdbjava.Dbi.KeyNotFoundException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import org.lmdbjava.LmdbNativeException.ConstantDerviedException;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.createBb;
import org.lmdbjava.Txn.CommittedException;

public class DbiTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi dbi;
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
    dbi = new Dbi(tx, DB_1, MDB_CREATE);
  }

  @Test(expected = DbFullException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void dbOpenMaxDatabases() throws Exception {
    new Dbi(tx, "another", MDB_CREATE);
  }

  @Test(expected = CommittedException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void dbTxCommitted() throws Exception {
    tx.commit();
    new Dbi(tx, "another", MDB_CREATE);
  }

  @Test
  public void putAbortGet() throws Exception {
    Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
    assertThat(db.getName(), is(DB_1));

    db.put(tx, createBb(5), createBb(5));
    tx.abort();

    try (Txn tx2 = new Txn(env)) {
      db.get(tx2, createBb(5));
      fail("key does not exist");
    } catch (ConstantDerviedException e) {
      assertThat(e.getResultCode(), is(22));
    }
  }

  @Test
  public void putAndGetAndDeleteWithInternalTx() throws Exception {
    Dbi db = new Dbi(tx, DB_1, MDB_CREATE);
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
    Dbi db = new Dbi(tx, DB_1, MDB_CREATE);

    db.put(tx, createBb(5), createBb(5));
    tx.commit();

    try (Txn tx2 = new Txn(env)) {
      ByteBuffer result = db.get(tx2, createBb(5));
      assertThat(result.getInt(), is(5));
    }
  }

  @Test
  public void putDelete() throws Exception {
    Dbi db = new Dbi(tx, DB_1, MDB_CREATE);

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
              dbi.put(createBb(random.nextInt()), createBb(random.nextInt()));
            } catch (CommittedException | LmdbNativeException | NotOpenException |
                     BufferNotDirectException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }
}
