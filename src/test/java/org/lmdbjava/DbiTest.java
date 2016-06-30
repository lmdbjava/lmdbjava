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
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.Collections.nCopies;
import java.util.Random;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Dbi.DbFullException;
import org.lmdbjava.Dbi.KeyExistsException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadWriteRequiredException;

public class DbiTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<ByteBuffer> env;

  @Before
  public void before() throws Exception {
    final File path = tmp.newFile();
    env = create()
      .setMapSize(1, ByteUnit.MEBIBYTES)
      .setMaxReaders(1)
      .setMaxDbs(2)
      .open(path, MDB_NOSUBDIR);
  }

  @Test(expected = DbFullException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void dbOpenMaxDatabases() {
    env.openDbi("db1 is OK", MDB_CREATE);
    env.openDbi("db2 is OK", MDB_CREATE);
    env.openDbi("db3 fails", MDB_CREATE);
  }

  @Test
  public void getName() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    assertThat(db.getName(), is(DB_1));
  }

  @Test(expected = KeyExistsException.class)
  public void keyExistsException() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5), MDB_NOOVERWRITE);
      db.put(txn, bb(5), bb(5), MDB_NOOVERWRITE);
    }
  }

  @Test
  public void putAbortGet() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      txn.abort();
    }

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      assertNull(db.get(txn, bb(5)));
    }
  }

  @Test
  public void putAndGetAndDeleteWithInternalTx() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    db.put(bb(5), bb(5));
    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertNotNull(found);
      assertThat(txn.val().getInt(), is(5));
    }
    db.delete(bb(5));

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      assertNull(db.get(txn, bb(5)));
    }
  }

  @Test
  public void putCommitGet() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      txn.commit();
    }

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertNotNull(found);
      assertThat(txn.val().getInt(), is(5));
    }
  }

  @Test
  public void putDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      db.delete(txn, bb(5));

      assertNull(db.get(txn, bb(5)));
      txn.abort();
    }
  }

  @Test
  public void putDuplicateDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      db.put(txn, bb(5), bb(6));
      db.put(txn, bb(5), bb(7));
      db.delete(txn, bb(5), bb(6));

      try (final Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
        final ByteBuffer key = bb(5);
        cursor.get(key, MDB_SET_KEY);
        assertThat(cursor.count(), is(2L));
      }
      txn.abort();
    }
  }

  @Test
  public void putReserve() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final ByteBuffer key = bb(5);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      assertNull(db.get(txn, key));
      final ByteBuffer val = db.reserve(txn, key, 32);
      val.putLong(Long.MAX_VALUE);
      assertNotNull(db.get(txn, key));
      txn.commit();
    }
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val.capacity(), is(32));
      assertThat(val.getLong(), is(Long.MAX_VALUE));
      assertThat(val.getLong(8), is(0L));
    }
  }

  @Test(expected = MapFullException.class)
  public void testMapFullException() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer v = allocateDirect(1_024 * 1_024 * 1_024);
      db.put(txn, bb(1), v);
    }
  }

  @Test
  public void testParallelWritesStress() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream()
        .forEach(ignored -> {
          Random random = new Random();
          for (int i = 0; i < 15_000; i++) {
            try {
              db.put(bb(random.nextInt()), bb(random.nextInt()));
            } catch (CommittedException | LmdbNativeException | NotOpenException |
                     ReadWriteRequiredException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }
}
