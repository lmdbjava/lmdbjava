package org.lmdbjava;

import java.io.File;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.POSIX_MODE;

public class EnvTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected = AlreadyOpenException.class)
  public void cannotOpenTwice() throws Exception {
    final Env e = new Env();
    final File path = tmp.newFile();
    e.open(path, POSIX_MODE, MDB_NOSUBDIR);
    e.open(path, POSIX_MODE, MDB_NOSUBDIR); // error
  }

  @Test
  public void createAsDirectory() throws Exception {
    final Env env = new Env();
    assertThat(env, is(notNullValue()));
    assertThat(env.isOpen(), is(false));

    final File path = tmp.newFolder();
    env.open(path, POSIX_MODE);
    assertThat(env.isOpen(), is(true));
    assertThat(path.isDirectory(), is(true));
  }

  @Test
  public void createAsFile() throws Exception {
    final Env env = new Env();
    assertThat(env, is(notNullValue()));
    assertThat(env.isOpen(), is(false));
    final File path = tmp.newFile();
    env.setMapSize(1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    assertThat(env.isOpen(), is(true));
    assertThat(path.isFile(), is(true));
  }

}
