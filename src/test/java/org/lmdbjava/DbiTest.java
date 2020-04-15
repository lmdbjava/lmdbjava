/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
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
import static java.lang.Long.MAX_VALUE;
import static java.lang.System.getProperty;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.ArrayList;
import static java.util.Collections.nCopies;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import org.agrona.concurrent.UnsafeBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import org.lmdbjava.Dbi.DbFullException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DbiFlags.MDB_REVERSEKEY;
import org.lmdbjava.Env.MapFullException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.KeyRange.atMost;
import org.lmdbjava.LmdbNativeException.ConstantDerivedException;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.ba;
import static org.lmdbjava.TestUtils.bb;

/**
 * Test {@link Dbi}.
 */
public final class DbiTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<ByteBuffer> env;

  @After
  public void after() {
    env.close();
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = create()
        .setMapSize(MEBIBYTES.toBytes(64))
        .setMaxReaders(2)
        .setMaxDbs(2)
        .open(path, MDB_NOSUBDIR);
  }

  @Test(expected = ConstantDerivedException.class)
  public void close() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    db.put(bb(1), bb(42));
    db.close();
    db.put(bb(2), bb(42)); // error
  }

  @Test
  public void customComparator() {
    final Comparator<ByteBuffer> reverseOrder = (o1, o2) -> {
      final int lexicalOrder = ByteBufferProxy.PROXY_OPTIMAL.compare(o1, o2);
      if (lexicalOrder == 0) {
        return 0;
      }
      return lexicalOrder * -1;
    };
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, reverseOrder, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(db.put(txn, bb(2), bb(3)), is(true));
      assertThat(db.put(txn, bb(4), bb(6)), is(true));
      assertThat(db.put(txn, bb(6), bb(7)), is(true));
      assertThat(db.put(txn, bb(8), bb(7)), is(true));
      txn.commit();
    }
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> iter = db.iterate(txn, atMost(bb(4)))) {
      assertThat(iter.next().key(), is(bb(8)));
      assertThat(iter.next().key(), is(bb(6)));
      assertThat(iter.next().key(), is(bb(4)));
    }
  }

  @Test(expected = DbFullException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void dbOpenMaxDatabases() {
    env.openDbi("db1 is OK", MDB_CREATE);
    env.openDbi("db2 is OK", MDB_CREATE);
    env.openDbi("db3 fails", MDB_CREATE);
  }

  @Test
  @SuppressWarnings("PMD.DoNotUseThreads")
  public void dbiWithComparatorThreadSafety() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, PROXY_OPTIMAL::compare,
                                           MDB_CREATE);

    final List<Integer> keys = range(0, 1000).boxed().collect(toList());

    final ExecutorService pool = Executors.newCachedThreadPool();
    final AtomicBoolean proceed = new AtomicBoolean(true);
    final Future<?> reader = pool.submit(() -> {
      while (proceed.get()) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
          db.get(txn, bb(50));
        }
      }
    });

    for (final Integer key : keys) {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        db.put(txn, bb(key), bb(3));
        txn.commit();
      }
    }

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> iter = db.iterate(txn)) {

      final List<Integer> result = new ArrayList<>();
      while (iter.hasNext()) {
        result.add(iter.next().key().getInt());
      }

      assertThat(result, Matchers.contains(keys.toArray(new Integer[0])));
    }

    proceed.set(false);
    try {
      reader.get(1, SECONDS);
      pool.shutdown();
      pool.awaitTermination(1, SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void drop() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(1), bb(42));
      db.put(txn, bb(2), bb(42));
      assertThat(db.get(txn, bb(1)), not(nullValue()));
      assertThat(db.get(txn, bb(2)), not(nullValue()));
      db.drop(txn);
      assertThat(db.get(txn, bb(1)), is(nullValue())); // data gone
      assertThat(db.get(txn, bb(2)), is(nullValue()));
      db.put(txn, bb(1), bb(42)); // ensure DB still works
      db.put(txn, bb(2), bb(42));
      assertThat(db.get(txn, bb(1)), not(nullValue()));
      assertThat(db.get(txn, bb(2)), not(nullValue()));
    }
  }

  @Test
  public void dropAndDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    final Dbi<ByteBuffer> nameDb = env.openDbi((byte[]) null);
    final byte[] dbNameBytes = DB_1.getBytes(UTF_8);
    final ByteBuffer dbNameBuffer = allocateDirect(dbNameBytes.length);
    dbNameBuffer.put(dbNameBytes).flip();

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(nameDb.get(txn, dbNameBuffer), not(nullValue()));
      db.drop(txn, true);
      assertThat(nameDb.get(txn, dbNameBuffer), is(nullValue()));
      txn.commit();
    }
  }

  @Test
  public void dropAndDeleteAnonymousDb() {
    env.openDbi(DB_1, MDB_CREATE);
    final Dbi<ByteBuffer> nameDb = env.openDbi((byte[]) null);
    final byte[] dbNameBytes = DB_1.getBytes(UTF_8);
    final ByteBuffer dbNameBuffer = allocateDirect(dbNameBytes.length);
    dbNameBuffer.put(dbNameBytes).flip();

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(nameDb.get(txn, dbNameBuffer), not(nullValue()));
      nameDb.drop(txn, true);
      assertThat(nameDb.get(txn, dbNameBuffer), is(nullValue()));
      txn.commit();
    }

    nameDb.close(); // explicit close after drop is OK
  }

  @Test
  public void getName() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    assertThat(db.getName(), is(DB_1.getBytes(UTF_8)));
  }

  @Test
  public void getNamesWhenDbisPresent() {
    final byte[] dbHello = new byte[]{'h', 'e', 'l', 'l', 'o'};
    final byte[] dbWorld = new byte[]{'w', 'o', 'r', 'l', 'd'};
    env.openDbi(dbHello, MDB_CREATE);
    env.openDbi(dbWorld, MDB_CREATE);
    final List<byte[]> dbiNames = env.getDbiNames();
    assertThat(dbiNames, hasSize(2));
    assertThat(dbiNames.get(0), is(dbHello));
    assertThat(dbiNames.get(1), is(dbWorld));
  }

  @Test
  public void getNamesWhenEmpty() {
    final List<byte[]> dbiNames = env.getDbiNames();
    assertThat(dbiNames, empty());
  }

  @Test
  public void putAbortGet() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      txn.abort();
    }

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertNull(db.get(txn, bb(5)));
    }
  }

  @Test
  public void putAndGetAndDeleteWithInternalTx() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    db.put(bb(5), bb(5));
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertNotNull(found);
      assertThat(txn.val().getInt(), is(5));
    }
    assertThat(db.delete(bb(5)), is(true));
    assertThat(db.delete(bb(5)), is(false));

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertNull(db.get(txn, bb(5)));
    }
  }

  @Test
  public void putCommitGet() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertNotNull(found);
      assertThat(txn.val().getInt(), is(5));
    }
  }

  @Test
  public void putCommitGetByteArray() throws IOException {
    final File path = tmp.newFile();
    try (Env<byte[]> envBa = create(new ByteArrayProxy())
        .setMapSize(MEBIBYTES.toBytes(64))
        .setMaxReaders(1)
        .setMaxDbs(2)
        .open(path, MDB_NOSUBDIR)) {
      final Dbi<byte[]> db = envBa.openDbi(DB_1, MDB_CREATE);
      try (Txn<byte[]> txn = envBa.txnWrite()) {
        db.put(txn, ba(5), ba(5));
        txn.commit();
      }
      try (Txn<byte[]> txn = envBa.txnWrite()) {
        final byte[] found = db.get(txn, ba(5));
        assertNotNull(found);
        assertThat(new UnsafeBuffer(txn.val()).getInt(0), is(5));
      }
    }
  }

  @Test
  public void putDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      assertThat(db.delete(txn, bb(5)), is(true));

      assertNull(db.get(txn, bb(5)));
      txn.abort();
    }
  }

  @Test
  public void putDuplicateDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      db.put(txn, bb(5), bb(6));
      db.put(txn, bb(5), bb(7));
      assertThat(db.delete(txn, bb(5), bb(6)), is(true));
      assertThat(db.delete(txn, bb(5), bb(6)), is(false));
      assertThat(db.delete(txn, bb(5), bb(5)), is(true));
      assertThat(db.delete(txn, bb(5), bb(5)), is(false));

      try (Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
        final ByteBuffer key = bb(5);
        cursor.get(key, MDB_SET_KEY);
        assertThat(cursor.count(), is(1L));
      }
      txn.abort();
    }
  }

  @Test
  public void putReserve() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final ByteBuffer key = bb(5);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertNull(db.get(txn, key));
      final ByteBuffer val = db.reserve(txn, key, 32, MDB_NOOVERWRITE);
      val.putLong(MAX_VALUE);
      assertNotNull(db.get(txn, key));
      txn.commit();
    }
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val.capacity(), is(32));
      assertThat(val.getLong(), is(MAX_VALUE));
      assertThat(val.getLong(8), is(0L));
    }
  }

  @Test
  public void putZeroByteValueForNonMdbDupSortDatabase() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = allocateDirect(0);
      db.put(txn, bb(5), val);
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertNotNull(found);
      assertThat(txn.val().capacity(), is(0));
    }
  }

  @Test
  public void returnValueForNoDupData() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      // ok
      assertThat(db.put(txn, bb(5), bb(6), MDB_NODUPDATA), is(true));
      assertThat(db.put(txn, bb(5), bb(7), MDB_NODUPDATA), is(true));
      assertThat(db.put(txn, bb(5), bb(6), MDB_NODUPDATA), is(false));
    }
  }

  @Test
  public void returnValueForNoOverwrite() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      // ok
      assertThat(db.put(txn, bb(5), bb(6), MDB_NOOVERWRITE), is(true));
      // fails, but gets exist val
      assertThat(db.put(txn, bb(5), bb(8), MDB_NOOVERWRITE), is(false));
      assertThat(txn.val().getInt(0), is(6));
    }
  }

  @Test
  public void stats() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    db.put(bb(1), bb(42));
    db.put(bb(2), bb(42));
    db.put(bb(3), bb(42));
    final Stat stat;
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      stat = db.stat(txn);
    }
    assertThat(stat, is(notNullValue()));
    assertThat(stat.branchPages, is(0L));
    assertThat(stat.depth, is(1));
    assertThat(stat.entries, is(3L));
    assertThat(stat.leafPages, is(1L));
    assertThat(stat.overflowPages, is(0L));
    assertThat(stat.pageSize, is(4_096));
  }

  @Test(expected = MapFullException.class)
  @SuppressWarnings("PMD.PreserveStackTrace")
  public void testMapFullException() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer v;
      try {
        v = allocateDirect(1_024 * 1_024 * 1_024);
      } catch (final OutOfMemoryError e) {
        // Travis CI OS X build cannot allocate this much memory, so assume OK
        throw new MapFullException();
      }
      db.put(txn, bb(1), v);
    }
  }

  @Test
  public void testParallelWritesStress() {
    if (getProperty("os.name").startsWith("Windows")) {
      return; // Windows VMs run this test too slowly
    }

    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream()
        .forEach(ignored -> {
          final Random random = new Random();
          for (int i = 0; i < 15_000; i++) {
            db.put(bb(random.nextInt()), bb(random.nextInt()));
          }
        });
  }

  @Test
  public void listsFlags() {
    final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT, MDB_REVERSEKEY);

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final List<DbiFlags> flags = dbi.listFlags(txn);
      assertThat(flags, containsInAnyOrder(MDB_DUPSORT, MDB_REVERSEKEY));
    }
  }
}
