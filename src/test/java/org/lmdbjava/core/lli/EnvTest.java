package org.lmdbjava.core.lli;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.core.lli.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.core.lli.TestUtils.POSIX_MODE;

public class EnvTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected = AlreadyOpenException.class)
  public void cannotOpenTwice() throws Exception {
    final Env e = new Env();
    final File path = tmp.newFile();
    final Set<EnvFlags> flags = new HashSet<>();
    flags.add(MDB_NOSUBDIR);
    e.open(path, flags, POSIX_MODE);
    e.open(path, flags, POSIX_MODE); // error
  }

  @Test
  public void createAsDirectory() throws Exception {
    final Env env = new Env();
    assertThat(env, is(notNullValue()));
    assertThat(env.isOpen(), is(false));

    final File path = tmp.newFolder();
    final Set<EnvFlags> flags = new HashSet<>();

    env.open(path, flags, POSIX_MODE);
    assertThat(env.isOpen(), is(true));
    assertThat(path.isDirectory(), is(true));
  }

  @Test
  public void createAsFile() throws Exception {
    final Env env = new Env();
    assertThat(env, is(notNullValue()));
    assertThat(env.isOpen(), is(false));
    final File path = tmp.newFile();
    final Set<EnvFlags> flags = new HashSet<>();
    flags.add(MDB_NOSUBDIR);
    env.setMapSize(1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, flags, POSIX_MODE);
    assertThat(env.isOpen(), is(true));
    assertThat(path.isFile(), is(true));
  }

}
