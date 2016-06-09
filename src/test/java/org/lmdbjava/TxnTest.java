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
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ResetException;
import org.lmdbjava.Txn.NotResetException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;
import static org.lmdbjava.TestUtils.createBb;

public class TxnTest {

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
    Txn tx = new Txn(env);
    Database db = new Database(tx, DB_1, MDB_CREATE);
    tx.commit();

    final AtomicLong txId1 = new AtomicLong();
    final AtomicLong txId2 = new AtomicLong();

    try (Txn tx1 = new Txn(env, MDB_RDONLY)) {
      txId1.set(tx1.getId());
    }

    db.put(createBb(1), createBb(2));

    try (Txn tx2 = new Txn(env, MDB_RDONLY)) {
      txId2.set(tx2.getId());
    }
    // should not see the same snapshot
    assertThat(txId1.get(), is(not(txId2.get())));
  }

  @Test
  public void txCanCommitThenCloseWithoutError() throws Exception {
    try (Txn tx = new Txn(env, MDB_RDONLY)) {
      assertThat(tx.isCommitted(), is(false));
      tx.commit();
      assertThat(tx.isCommitted(), is(true));
    }
  }

  @Test(expected = CommittedException.class)
  public void txCannotAbortIfAlreadyCommitted() throws Exception {
    try (Txn tx = new Txn(env, MDB_RDONLY)) {
      assertThat(tx.isCommitted(), is(false));
      tx.commit();
      assertThat(tx.isCommitted(), is(true));
      tx.abort();
    }
  }

  @Test(expected = CommittedException.class)
  public void txCannotCommitTwice() throws Exception {
    final Txn tx = new Txn(env);
    tx.commit();
    tx.commit(); // error
  }

  @Test(expected = NotOpenException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void txConstructionDeniedIfEnvClosed() throws Exception {
    env.close();
    new Txn(env);
  }

  @Test
  public void txParent() throws Exception {
    final Txn txRoot = new Txn(env);
    final Txn txChild = new Txn(env, txRoot);
    assertThat(txRoot.getParent(), is(nullValue()));
    assertThat(txChild.getParent(), is(txRoot));
  }

  @Test
  public void txReadOnly() throws Exception {
    final Txn tx = new Txn(env, MDB_RDONLY);
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
  public void txReadWrite() throws Exception {
    final Txn tx = new Txn(env);
    assertThat(tx.getParent(), is(nullValue()));
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(false));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

  @Test(expected = NotResetException.class)
  public void txRenewDeniedWithoutPriorReset() throws Exception {
    final Txn tx = new Txn(env, MDB_RDONLY);
    tx.renew();
  }

  @Test(expected = ResetException.class)
  public void txResetDeniedForAlreadyResetTransaction() throws Exception {
    final Txn tx = new Txn(env, MDB_RDONLY);
    tx.reset();
    tx.renew();
    tx.reset();
    tx.reset();
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void txResetDeniedForReadWriteTransaction() throws Exception {
    final Txn tx = new Txn(env);
    tx.reset();
  }
}
