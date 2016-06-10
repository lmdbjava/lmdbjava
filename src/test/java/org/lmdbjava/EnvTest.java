package org.lmdbjava;

import java.io.File;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CopyFlags.MDB_CP_COMPACT;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.AlreadyOpenException;
import org.lmdbjava.Env.InvalidCopyDestination;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.POSIX_MODE;

public class EnvTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void canCloseBeforeOpen() throws Exception {
    final Env env = new Env();
    env.close();
    assertThat(env.isClosed(), is(true));
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotOpenTwice() throws Exception {
    final Env e = new Env();
    final File path = tmp.newFile();
    e.open(path, POSIX_MODE, MDB_NOSUBDIR);
    e.open(path, POSIX_MODE, MDB_NOSUBDIR); // error
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSetMapSizeOnceClosed() throws Exception {
    final Env env = new Env();
    env.close();
    env.setMapSize(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotSetMapSizeOnceOpen() throws Exception {
    final Env env = new Env();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.setMapSize(1);
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSetMaxDbsOnceClosed() throws Exception {
    final Env env = new Env();
    env.close();
    env.setMaxDbs(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotSetMaxDbsOnceOpen() throws Exception {
    final Env env = new Env();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.setMaxDbs(1);
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSetMaxReadersOnceClosed() throws Exception {
    final Env env = new Env();
    env.close();
    env.setMaxReaders(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotSetMaxReadersOnceOpen() throws Exception {
    final Env env = new Env();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.setMaxReaders(1);
  }

  @Test
  @Ignore(value = "Travis CI failure; suspect older liblmdb version")
  public void copy() throws Exception {
    final File dest = tmp.newFolder();
    assertThat(dest.exists(), is(true));
    assertThat(dest.isDirectory(), is(true));
    assertThat(dest.list().length, is(0));
    try (Env env = new Env()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
      assertThat(dest.list().length, is(1));
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsFileDestination() throws Exception {
    final File dest = tmp.newFile();
    try (Env env = new Env()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsMissingDestination() throws Exception {
    final File dest = tmp.newFolder();
    dest.delete();
    try (Env env = new Env()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsNonEmptyDestination() throws Exception {
    final File dest = tmp.newFolder();
    final File subDir = new File(dest, "hello");
    subDir.mkdir();
    try (Env env = new Env()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test
  public void createAsDirectory() throws Exception {
    final Env env = new Env();
    assertThat(env, is(notNullValue()));
    assertThat(env.isOpen(), is(false));
    assertThat(env.isClosed(), is(false));

    final File path = tmp.newFolder();
    env.open(path, POSIX_MODE);
    assertThat(env.isOpen(), is(true));
    assertThat(path.isDirectory(), is(true));
    env.sync(false);
    env.close();
    assertThat(env.isClosed(), is(true));
    env.close(); // safe to repeat
  }

  @Test
  public void createAsFile() throws Exception {
    try (Env env = new Env()) {
      assertThat(env, is(notNullValue()));
      assertThat(env.isOpen(), is(false));
      final File path = tmp.newFile();
      env.setMapSize(1_024 * 1_024);
      env.setMaxDbs(1);
      env.setMaxReaders(1);
      env.open(path, POSIX_MODE, MDB_NOSUBDIR);
      env.sync(true);
      assertThat(env.isOpen(), is(true));
      assertThat(path.isFile(), is(true));
    }
  }

  @Test
  public void info() throws Exception {
    final Env env = new Env();
    final File path = tmp.newFile();
    env.setMaxReaders(4);
    env.setMapSize(123_456);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    EnvInfo info = env.info();
    assertThat(info, is(notNullValue()));
    assertThat(info.lastPageNumber, is(1L));
    assertThat(info.lastTransactionId, is(0L));
    assertThat(info.mapAddress, is(0L));
    assertThat(info.mapSize, is(123_456L));
    assertThat(info.maxReaders, is(4));
    assertThat(info.numReaders, is(0));
  }

  @Test
  public void stats() throws Exception {
    final Env env = new Env();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    EnvStat stat = env.stat();
    assertThat(stat, is(notNullValue()));
    assertThat(stat.branchPages, is(0L));
    assertThat(stat.depth, is(0));
    assertThat(stat.entries, is(0L));
    assertThat(stat.leafPages, is(0L));
    assertThat(stat.overflowPages, is(0L));
    assertThat(stat.pageSize, is(4_096));
  }
}
