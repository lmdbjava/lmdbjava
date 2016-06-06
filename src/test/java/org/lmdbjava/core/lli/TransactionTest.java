package org.lmdbjava.core.lli;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.core.lli.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.core.lli.TestUtils.POSIX_MODE;

public class TransactionTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env env;

  @Before
  public void before() throws Exception {
    env = new Env();
    final File path = tmp.newFile();

    final Set<EnvFlags> flags = new HashSet<>();
    flags.add(MDB_NOSUBDIR);

    env.open(path, flags, POSIX_MODE);
  }

  @Test(expected = AlreadyCommittedException.class)
  public void txCannotCommitTwice() throws Exception {
    final Transaction tx = env.tnxBeginReadWrite();
    tx.commit();
    tx.commit(); // error
  }

  @Test
  public void txReadOnly() throws Exception {
    final Transaction tx = env.tnxBeginReadOnly();
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(true));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

  @Test
  public void txReadWrite() throws Exception {
    final Transaction tx = env.tnxBeginReadWrite();
    assertThat(tx.isCommitted(), is(false));
    assertThat(tx.isReadOnly(), is(false));
    tx.commit();
    assertThat(tx.isCommitted(), is(true));
  }

}
