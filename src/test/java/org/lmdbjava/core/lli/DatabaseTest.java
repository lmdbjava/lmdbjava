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

    tx = env.tnxBeginReadWrite();
  }

  @Test
  public void dbOpen() throws Exception {
    Set<DatabaseFlags> dbFlags = new HashSet<>();
    dbFlags.add(MDB_CREATE);
    Database db = tx.databaseOpen(DB_1, dbFlags);
    assertThat(db.getName(), is(DB_1));
  }

}
