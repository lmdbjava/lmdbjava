package org.lmdbjava;

import java.io.File;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.POSIX_MODE;

public class TransactionTest {

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

  @Test(expected = AlreadyCommittedException.class)
  public void txCannotCommitTwice() throws Exception {
    final Transaction tx = env.txnBeginReadWrite();
    tx.commit();
    tx.commit(); // error
  }

  @Test
  public void txReadOnly() throws Exception {
    final Transaction tx = env.txnBeginReadOnly();
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(true));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

  @Test
  public void txReadWrite() throws Exception {
    final Transaction tx = env.txnBeginReadWrite();
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(false));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

//  @Test
//  public void testGetId() throws Exception {
//    Set<DatabaseFlags> dbFlags = new HashSet<>();
//    dbFlags.add(MDB_CREATE);
//    Transaction tx = env.txnBeginReadWrite();
//    Database db = tx.databaseOpen(DB_1, dbFlags);
//    tx.commit();
//
//    final AtomicLong txId1 = new AtomicLong();
//    final AtomicLong txId2 = new AtomicLong();
//
//    try (Transaction tx1 = env.txnBeginReadOnly()) {
//      txId1.set(tx1.getId());
//    }
//
//    db.put(createBb(1), createBb(2));
//
//    try (Transaction tx2 = env.txnBeginReadOnly()) {
//      txId2.set(tx2.getId());
//    }
//    // should not see the same snapshot
//    Assert.assertThat(txId1.get(), Is.is(not(txId2.get())));
//  }
}
