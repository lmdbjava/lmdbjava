/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CopyFlags.MDB_CP_COMPACT;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.AlreadyOpenException;
import org.lmdbjava.Env.Builder;
import org.lmdbjava.Env.InvalidCopyDestination;
import org.lmdbjava.Env.MapFullException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Env.open;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;
import org.lmdbjava.Txn.BadReaderLockException;


/**
 * Test {@link Env}.
 */
public final class EnvTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void byteUnit() throws IOException {
    final File path = tmp.newFile();
    try (Env<ByteBuffer> env
        = create().setMapSize(MEBIBYTES.toBytes(1)).open(
            path, MDB_NOSUBDIR)) {
      final EnvInfo info = env.info();
      assertThat(info.mapSize, is(MEBIBYTES.toBytes(1)));
    }
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotChangeMapSizeAfterOpen() throws IOException {
    final File path = tmp.newFile();
    final Builder<ByteBuffer> builder = create();
    builder.open(path, MDB_NOSUBDIR);
    builder.setMapSize(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotChangeMaxDbsAfterOpen() throws IOException {
    final File path = tmp.newFile();
    final Builder<ByteBuffer> builder = create();
    builder.open(path, MDB_NOSUBDIR);
    builder.setMaxDbs(1);
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotChangeMaxReadersAfterOpen() throws IOException {
    final File path = tmp.newFile();
    final Builder<ByteBuffer> builder = create();
    builder.open(path, MDB_NOSUBDIR);
    builder.setMaxReaders(1);
  }

  @Test(expected = AlreadyClosedException.class)
  public void cannotInfoOnceClosed() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
        .open(path, MDB_NOSUBDIR);
    env.close();
    env.info();
  }

  @Test(expected = AlreadyOpenException.class)
  public void cannotOpenTwice() throws IOException {
    final File path = tmp.newFile();
    final Builder<ByteBuffer> builder = create();
    builder.open(path, MDB_NOSUBDIR);
    builder.open(path, MDB_NOSUBDIR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void cannotOverflowMapSize() {
    final Builder<ByteBuffer> builder = create();
    final int mb = 1_024 * 1_024;
    final int size = mb * 2_048; // as per issue 18
    builder.setMapSize(size);
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
  public void copy() throws IOException {
    final File dest = tmp.newFolder();
    assertThat(dest.exists(), is(true));
    assertThat(dest.isDirectory(), is(true));
    assertThat(dest.list().length, is(0));
    final File src = tmp.newFolder();
    try (Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
      assertThat(dest.list().length, is(1));
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsFileDestination() throws IOException {
    final File dest = tmp.newFile();
    final File src = tmp.newFolder();
    try (Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsMissingDestination() throws IOException {
    final File dest = tmp.newFolder();
    assertThat(dest.delete(), is(true));
    final File src = tmp.newFolder();
    try (Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test(expected = InvalidCopyDestination.class)
  public void copyRejectsNonEmptyDestination() throws IOException {
    final File dest = tmp.newFolder();
    final File subDir = new File(dest, "hello");
    assertThat(subDir.mkdir(), is(true));
    final File src = tmp.newFolder();
    try (Env<ByteBuffer> env = create().open(src)) {
      env.copy(dest, MDB_CP_COMPACT);
    }
  }

  @Test
  public void createAsDirectory() throws IOException {
    final File path = tmp.newFolder();
    final Env<ByteBuffer> env = create().open(path);
    assertThat(path.isDirectory(), is(true));
    env.sync(false);
    env.close();
    assertThat(env.isClosed(), is(true));
    env.close(); // safe to repeat
  }

  @Test
  public void createAsFile() throws IOException {
    final File path = tmp.newFile();
    try (Env<ByteBuffer> env = create()
        .setMapSize(1_024 * 1_024)
        .setMaxDbs(1)
        .setMaxReaders(1)
        .open(path, MDB_NOSUBDIR)) {
      env.sync(true);
      assertThat(path.isFile(), is(true));
    }
  }

  @Test(expected = BadReaderLockException.class)
  public void detectTransactionThreadViolation() throws IOException {
    final File path = tmp.newFile();
    final Env<ByteBuffer> env = create()
        .open(path, MDB_NOSUBDIR);
    env.txnRead();
    env.txnRead();
  }

  @Test
  public void info() throws IOException {
    final File path = tmp.newFile();
    try (Env<ByteBuffer> env = create()
        .setMaxReaders(4)
        .setMapSize(123_456)
        .open(path, MDB_NOSUBDIR)) {
      final EnvInfo info = env.info();
      assertThat(info, is(notNullValue()));
      assertThat(info.lastPageNumber, is(1L));
      assertThat(info.lastTransactionId, is(0L));
      assertThat(info.mapAddress, is(0L));
      assertThat(info.mapSize, is(123_456L));
      assertThat(info.maxReaders, is(4));
      assertThat(info.numReaders, is(0));
      assertThat(env.getMaxKeySize(), is(511));
    }
  }

  @Test(expected = MapFullException.class)
  public void mapFull() throws IOException {
    final File path = tmp.newFolder();
    final byte[] k = new byte[500];
    final ByteBuffer key = allocateDirect(500);
    final ByteBuffer val = allocateDirect(1_024);
    final Random rnd = new Random();
    try (Env<ByteBuffer> env = create().setMapSize(MEBIBYTES.toBytes(8))
        .setMaxDbs(1).open(path)) {
      final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
      for (;;) {
        rnd.nextBytes(k);
        key.clear();
        key.put(k).flip();
        val.clear();
        db.put(key, val);
      }
    }
  }

  @Test
  public void readOnlySupported() throws IOException {
    final File path = tmp.newFolder();
    try (Env<ByteBuffer> rwEnv = create().open(path)) {
      final Dbi<ByteBuffer> rwDb = rwEnv.openDbi(DB_1, MDB_CREATE);
      rwDb.put(bb(1), bb(42));
    }
    try (Env<ByteBuffer> roEnv = create().open(path, MDB_RDONLY_ENV)) {
      final Dbi<ByteBuffer> roDb = roEnv.openDbi(DB_1);
      try (Txn<ByteBuffer> roTxn = roEnv.txnRead()) {
        assertThat(roDb.get(roTxn, bb(1)), notNullValue());
      }
    }
  }

  @Test
  public void stats() throws IOException {
    final File path = tmp.newFile();
    try (Env<ByteBuffer> env = create().open(path, MDB_NOSUBDIR)) {
      final Stat stat = env.stat();
      assertThat(stat, is(notNullValue()));
      assertThat(stat.branchPages, is(0L));
      assertThat(stat.depth, is(0));
      assertThat(stat.entries, is(0L));
      assertThat(stat.leafPages, is(0L));
      assertThat(stat.overflowPages, is(0L));
      assertThat(stat.pageSize, is(4_096));
    }
  }

  @Test
  public void testDefaultOpen() throws IOException {
    final File path = tmp.newFolder();
    try (Env<ByteBuffer> env = open(path, 10)) {
      final Dbi<ByteBuffer> db = env.openDbi("test", MDB_CREATE);
      db.put(allocateDirect(1), allocateDirect(1));
    }
  }

  @Test
  public void testDbiNames() throws IOException {
    final File path = tmp.newFolder();
    try (Env<ByteBuffer> env = create().setMaxDbs(10).open(path)) {
      assertThat(env.allDbiNames(StandardCharsets.UTF_8),
          is(Collections.emptyList()));
      // the third string is a UTF-16 surrogate pair, which should
      // uncover encoding bugs that ASCII strings will not
      final String[] names = new String[] {"one", "two", "𤭢"};
      env.openDbi(names[0], StandardCharsets.UTF_8, MDB_CREATE);
      env.openDbi(names[1], StandardCharsets.UTF_8, MDB_CREATE);
      env.openDbi(names[2], StandardCharsets.UTF_8, MDB_CREATE);
      assertThat(env.allDbiNames(StandardCharsets.UTF_8),
          is(Arrays.asList(names)));
    }
  }

  @Test
  public void testOpenAll() throws IOException {
    final File path = tmp.newFolder();
    // the third string is a UTF-16 surrogate pair, which should
    // uncover encoding bugs that ASCII strings will not
    final String[] names = new String[] {"one", "two", "𤭢"};
    try (Env<ByteBuffer> env = create().setMaxDbs(10).open(path)) {
      assertThat(env.openAllDbi(StandardCharsets.UTF_8),
          is(Collections.emptyMap()));
      env.openDbi(names[0], StandardCharsets.UTF_8, MDB_CREATE);
      env.openDbi(names[1], StandardCharsets.UTF_8, MDB_CREATE);
      env.openDbi(names[2], StandardCharsets.UTF_8, MDB_CREATE);
      final Map<String, Dbi<ByteBuffer>> dbiMap
          = env.openAllDbi(StandardCharsets.UTF_8);
      assertThat(dbiMap.size(), is(3));
      assertThat(dbiMap.get(names[0]), isA(Dbi.class));
      assertThat(dbiMap.get(names[1]), isA(Dbi.class));
      assertThat(dbiMap.get(names[2]), isA(Dbi.class));
    }

    try (Env<ByteBuffer> env
        = create().setMaxDbs(10).open(path, MDB_RDONLY_ENV)) {
      final Map<String, Dbi<ByteBuffer>> dbiMap
          = env.openAllDbi(StandardCharsets.UTF_8);
      assertThat(dbiMap.size(), is(3));
      assertThat(dbiMap.get(names[0]), isA(Dbi.class));
      assertThat(dbiMap.get(names[0]).getName(), is(names[0]));
      assertThat(dbiMap.get(names[1]), isA(Dbi.class));
      assertThat(dbiMap.get(names[1]).getName(), is(names[1]));
      assertThat(dbiMap.get(names[2]), isA(Dbi.class));
      assertThat(dbiMap.get(names[2]).getName(), is(names[2]));
    }
  }
}
