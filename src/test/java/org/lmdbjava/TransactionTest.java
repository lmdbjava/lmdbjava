package org.lmdbjava;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.DatabaseFlags.MDB_CREATE;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.createBb;
import static org.lmdbjava.TransactionFlags.MDB_RDONLY;

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

  @Test
  @Ignore
  public void testGetId() throws Exception {
    Transaction tx = new Transaction(env, null);
    Database db = tx.databaseOpen(DB_1, MDB_CREATE);
    tx.commit();

    final AtomicLong txId1 = new AtomicLong();
    final AtomicLong txId2 = new AtomicLong();

    try (Transaction tx1 = new Transaction(env, null, MDB_RDONLY)) {
      txId1.set(tx1.getId());
    }

    db.put(createBb(1), createBb(2));

    try (Transaction tx2 = new Transaction(env, null, MDB_RDONLY)) {
      txId2.set(tx2.getId());
    }
    // should not see the same snapshot
    assertThat(txId1.get(), is(not(txId2.get())));
  }

  @Test(expected = AlreadyCommittedException.class)
  public void txCannotCommitTwice() throws Exception {
    final Transaction tx = new Transaction(env, null);
    tx.commit();
    tx.commit(); // error
  }

  @Test
  public void txReadOnly() throws Exception {
    final Transaction tx = new Transaction(env, null, MDB_RDONLY);
    assertThat(tx.getParent(), is(nullValue()));
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(true));
    assertThat(tx.isReset(), is(false));
    tx.reset();
    assertThat(tx.isReset(), is(true));
    tx.renew();
    assertThat(tx.isReset(), is(false));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

  @Test
  public void txParent() throws Exception {
    final Transaction txRoot = new Transaction(env, null);
    final Transaction txChild = new Transaction(env, txRoot);
    assertThat(txRoot.getParent(), is(nullValue()));
    assertThat(txChild.getParent(), is(txRoot));
  }

  @Test
  public void txReadWrite() throws Exception {
    final Transaction tx = new Transaction(env, null);
    assertThat(tx.getParent(), is(nullValue()));
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(false));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

}
