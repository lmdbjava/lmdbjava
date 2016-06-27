/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.POSIX_MODE;

public class EnvTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void canCloseBeforeOpen() {
    final Env<ByteBuffer> env = create();
    env.close();
    assertThat(env.isClosed(), is(true));
  }

  @Test(expected = NotOpenException.class)
  public void cannotInfoIfNeverOpen() {
    final Env<ByteBuffer> env = create();
    env.info();
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotInfoOnceClosed() {
    final Env<ByteBuffer> env = create();
    env.close();
    env.info();
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotOpenOnceClosed() throws IOException {
    final Env<ByteBuffer> env = create();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.close();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR); // error
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotOpenTwice() throws IOException {
    final Env<ByteBuffer> env = create();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR); // error
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSetMapSizeOnceClosed() {
    final Env<ByteBuffer> env = create();
    env.close();
    env.setMapSize(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotSetMapSizeOnceOpen() throws IOException {
    final Env<ByteBuffer> env = create();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.setMapSize(1);
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSetMaxDbsOnceClosed() {
    final Env<ByteBuffer> env = create();
    env.close();
    env.setMaxDbs(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotSetMaxDbsOnceOpen() throws IOException {
    final Env<ByteBuffer> env = create();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.setMaxDbs(1);
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSetMaxReadersOnceClosed() {
    final Env<ByteBuffer> env = create();
    env.close();
    env.setMaxReaders(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotSetMaxReadersOnceOpen() throws IOException {
    final Env<ByteBuffer> env = create();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.setMaxReaders(1);
  }

  @Test(expected = NotOpenException.class)
  public void cannotStatIfNeverOpen() {
    final Env<ByteBuffer> env = create();
    env.stat();
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotStatOnceClosed() {
    final Env<ByteBuffer> env = create();
    env.close();
    env.stat();
  }

  @Test(expected = NotOpenException.class)
  public void cannotSyncIfNotOpen() {
    final Env<ByteBuffer> env = create();
    env.sync(false);
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSyncOnceClosed() throws IOException {
    final Env<ByteBuffer> env = create();
    final File path = tmp.newFile();
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
    env.close();
    env.sync(false);
  }

  @Test
  @Ignore("Travis CI failure; suspect older liblmdb version")
  public void copy() throws IOException {
    final File dest = tmp.newFolder();
    assertThat(dest.exists(), is(true));
    assertThat(dest.isDirectory(), is(true));
    assertThat(dest.list().length, is(0));
    try (final Env<ByteBuffer> env = create()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
      assertThat(dest.list().length, is(1));
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsFileDestination() throws IOException {
    final File dest = tmp.newFile();
    try (final Env<ByteBuffer> env = create()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsMissingDestination() throws IOException {
    final File dest = tmp.newFolder();
    dest.delete();
    try (final Env<ByteBuffer> env = create()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsNonEmptyDestination() throws IOException {
    final File dest = tmp.newFolder();
    final File subDir = new File(dest, "hello");
    subDir.mkdir();
    try (final Env<ByteBuffer> env = create()) {
      final File src = tmp.newFolder();
      env.open(src, POSIX_MODE);
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test
  public void createAsDirectory() throws IOException {
    final Env<ByteBuffer> env = create();
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
  public void createAsFile() throws IOException {
    try (final Env<ByteBuffer> env = create()) {
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
  public void info() throws IOException {
    final Env<ByteBuffer> env = create();
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
  public void stats() throws IOException {
    final Env<ByteBuffer> env = create();
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
