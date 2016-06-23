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
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CursorOp.MDB_SET_KEY;
import org.lmdbjava.Dbi.DbFullException;
import org.lmdbjava.Dbi.KeyExistsException;
import org.lmdbjava.Dbi.KeyNotFoundException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.allocateBb;
import static org.lmdbjava.TestUtils.createBb;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.ReadWriteRequiredException;

public class DbiTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env env;

  @Before
  public void before() throws Exception {
    env = new Env();
    final File path = tmp.newFile();

    env.setMapSize(1_024 * 1_024 * 1_024);
    env.setMaxDbs(2);
    env.setMaxReaders(1);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
  }

  @Test(expected = DbFullException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void dbOpenMaxDatabases() throws Exception {
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
    try (final Txn tx = new Txn(env)) {
      db.put(tx, createBb(5), createBb(5), MDB_NOOVERWRITE);
      db.put(tx, createBb(5), createBb(5), MDB_NOOVERWRITE);
    }
  }

  @Test
  public void putAbortGet() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (final Txn tx = new Txn(env)) {
      db.put(tx, createBb(5), createBb(5));
      tx.abort();
    }

    try (final Txn tx = new Txn(env)) {
      db.get(tx, createBb(5));
      fail("key does not exist");
    } catch (KeyNotFoundException e) {
    }
  }

  @Test
  public void putAndGetAndDeleteWithInternalTx() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    db.put(createBb(5), createBb(5));
    final ByteBuffer val = db.get(createBb(5));
    assertThat(val.getInt(), is(5));
    db.delete(createBb(5));

    try {
      db.get(createBb(5));
      fail("should have been deleted");
    } catch (KeyNotFoundException e) {
    }
  }

  @Test
  public void putCommitGet() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn tx = new Txn(env)) {
      db.put(tx, createBb(5), createBb(5));
      tx.commit();
    }

    try (final Txn tx = new Txn(env)) {
      final ByteBuffer result = db.get(tx, createBb(5));
      assertThat(result.getInt(), is(5));
    }
  }

  @Test
  public void putDelete() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (final Txn tx = new Txn(env)) {
      db.put(tx, createBb(5), createBb(5));
      db.delete(tx, createBb(5));

      try {
        db.get(tx, createBb(5));
        fail("key does not exist");
      } catch (KeyNotFoundException e) {
      }
      tx.abort();
    }
  }

  @Test
  public void putDuplicateDelete() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);

    try (final Txn tx = new Txn(env)) {
      db.put(tx, createBb(5), createBb(5));
      db.put(tx, createBb(5), createBb(6));
      db.put(tx, createBb(5), createBb(7));
      db.delete(tx, createBb(5), createBb(6));

      try (final Cursor<ByteBuffer> cursor = db.openCursor(tx)) {
        final ByteBuffer key = allocateBb(db, 5);
        cursor.get(key, MDB_SET_KEY);
        assertThat(cursor.count(), is(2L));
      }
      tx.abort();
    }
  }

  @Test(expected = MapFullException.class)
  public void testMapFullException() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (final Txn tx = new Txn(env)) {
      final ByteBuffer v = allocateDirect(1_024 * 1_024 * 1_024);
      db.put(tx, createBb(1), v);
    }
  }

  @Test
  public void testParallelWritesStress() throws Exception {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream()
        .forEach(ignored -> {
          Random random = new Random();
          for (int i = 0; i < 15_000; i++) {
            try {
              db.put(createBb(random.nextInt()), createBb(random.nextInt()));
            } catch (CommittedException | LmdbNativeException | NotOpenException |
                     ReadWriteRequiredException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }
}
