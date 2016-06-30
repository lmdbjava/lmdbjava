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

  @Test(expected = AlreadyClosedException.class)
  public void cannotInfoOnceClosed() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
      .open(path, MDB_NOSUBDIR);
    env.close();
    env.info();
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotStatOnceClosed() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
      .open(path, MDB_NOSUBDIR);
    env.close();
    env.stat();
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotSyncOnceClosed() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
      .open(path, MDB_NOSUBDIR);
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
    final File src = tmp.newFolder();
    try (final Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
      assertThat(dest.list().length, is(1));
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsFileDestination() throws IOException {
    final File dest = tmp.newFile();
    final File src = tmp.newFolder();
    try (final Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsMissingDestination() throws IOException {
    final File dest = tmp.newFolder();
    dest.delete();
    final File src = tmp.newFolder();
    try (final Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsNonEmptyDestination() throws IOException {
    final File dest = tmp.newFolder();
    final File subDir = new File(dest, "hello");
    subDir.mkdir();
    final File src = tmp.newFolder();
    try (final Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test
  public void createAsDirectory() throws IOException {
    final File path = tmp.newFolder();
    final Env<ByteBuffer> env = create().open(path);
    assertThat(env.isOpen(), is(true));
    assertThat(path.isDirectory(), is(true));
    env.sync(false);
    env.close();
    assertThat(env.isClosed(), is(true));
    env.close(); // safe to repeat
  }

  @Test
  public void createAsFile() throws IOException {
    final File path = tmp.newFile();
    try (final Env<ByteBuffer> env = create()
      .setMapSize(1_024 * 1_024)
      .setMaxDbs(1)
      .setMaxReaders(1)
      .open(path, MDB_NOSUBDIR)) {
      env.sync(true);
      assertThat(env.isOpen(), is(true));
      assertThat(path.isFile(), is(true));
    }
  }

  @Test
  public void info() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
      .setMaxReaders(4)
      .setMapSize(123_456)
      .open(path, MDB_NOSUBDIR);
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
  public void testDefaultOpen() {
    try (Env<ByteBuffer> env = Env.open(new File("/tmp"), 10)) {
      Dbi<ByteBuffer> db = env.openDbi("test", DbiFlags.MDB_CREATE);
      db.put(ByteBuffer.allocateDirect(1), ByteBuffer.allocateDirect(1));
    }
  }

  @Test
  public void byteUnit() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
      .setMapSize(1, ByteUnit.MEBIBYTES)
      .open(path, MDB_NOSUBDIR);
    EnvInfo info = env.info();
    assertThat(info.mapSize, is(ByteUnit.MEBIBYTES.toBytes(1)));
  }

  @Test
  public void stats() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
      .open(path, MDB_NOSUBDIR);
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
