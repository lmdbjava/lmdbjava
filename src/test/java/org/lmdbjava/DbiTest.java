/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static java.lang.Long.MAX_VALUE;
import static java.lang.System.getProperty;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DbiFlags.MDB_INTEGERKEY;
import static org.lmdbjava.DbiFlags.MDB_REVERSEKEY;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.KeyRange.atMost;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi.DbFullException;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.LmdbNativeException.ConstantDerivedException;

/** Test {@link Dbi}. */
public final class DbiTest {

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<ByteBuffer> env;
  private Env<byte[]> envBa;

  @After
  public void after() {
    env.close();
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env =
        create()
            .setMapSize(MEBIBYTES.toBytes(64))
            .setMaxReaders(2)
            .setMaxDbs(2)
            .open(path, MDB_NOSUBDIR);
    final File pathBa = tmp.newFile();
    envBa =
        create(PROXY_BA)
            .setMapSize(MEBIBYTES.toBytes(64))
            .setMaxReaders(2)
            .setMaxDbs(2)
            .open(pathBa, MDB_NOSUBDIR);
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
    final Comparator<ByteBuffer> reverseOrder =
        (o1, o2) -> {
          final int lexical = PROXY_OPTIMAL.getComparator().compare(o1, o2);
          if (lexical == 0) {
            return 0;
          }
          return lexical * -1;
        };
    doCustomComparator(env, reverseOrder, TestUtils::bb, ByteBuffer::getInt);
  }

  @Test
  public void customComparatorByteArray() {
    final Comparator<byte[]> reverseOrder =
        (o1, o2) -> {
          final int lexical = PROXY_BA.getComparator().compare(o1, o2);
          if (lexical == 0) {
            return 0;
          }
          return lexical * -1;
        };
    doCustomComparator(envBa, reverseOrder, TestUtils::ba, TestUtils::fromBa);
  }

  private <T> void doCustomComparator(
      Env<T> env,
      Comparator<T> comparator,
      IntFunction<T> serializer,
      ToIntFunction<T> deserializer) {
    final Dbi<T> db = env.openDbi(DB_1, comparator, true, MDB_CREATE);
    try (Txn<T> txn = env.txnWrite()) {
      assertThat(db.put(txn, serializer.apply(2), serializer.apply(3)), is(true));
      assertThat(db.put(txn, serializer.apply(4), serializer.apply(6)), is(true));
      assertThat(db.put(txn, serializer.apply(6), serializer.apply(7)), is(true));
      assertThat(db.put(txn, serializer.apply(8), serializer.apply(7)), is(true));
      txn.commit();
    }
    try (Txn<T> txn = env.txnRead();
        CursorIterable<T> ci = db.iterate(txn, atMost(serializer.apply(4)))) {
      final Iterator<KeyVal<T>> iter = ci.iterator();
      assertThat(deserializer.applyAsInt(iter.next().key()), is(8));
      assertThat(deserializer.applyAsInt(iter.next().key()), is(6));
      assertThat(deserializer.applyAsInt(iter.next().key()), is(4));
    }
  }

  @Test(expected = DbFullException.class)
  public void dbOpenMaxDatabases() {
    env.openDbi("db1 is OK", MDB_CREATE);
    env.openDbi("db2 is OK", MDB_CREATE);
    env.openDbi("db3 fails", MDB_CREATE);
  }

  @Test
  public void dbiWithComparatorThreadSafety() {
    doDbiWithComparatorThreadSafety(
        env, PROXY_OPTIMAL::getComparator, TestUtils::bb, ByteBuffer::getInt);
  }

  @Test
  public void dbiWithComparatorThreadSafetyByteArray() {
    doDbiWithComparatorThreadSafety(
        envBa, PROXY_BA::getComparator, TestUtils::ba, TestUtils::fromBa);
  }

  public <T> void doDbiWithComparatorThreadSafety(
      Env<T> env,
      Supplier<Comparator<T>> comparatorSupplier,
      IntFunction<T> serializer,
      ToIntFunction<T> deserializer) {
    final DbiFlags[] flags = new DbiFlags[] {MDB_CREATE, MDB_INTEGERKEY};
    final Comparator<T> c = comparatorSupplier.get();
    final Dbi<T> db = env.openDbi(DB_1, c, true, flags);

    final List<Integer> keys = range(0, 1_000).boxed().collect(toList());

    final ExecutorService pool = Executors.newCachedThreadPool();
    final AtomicBoolean proceed = new AtomicBoolean(true);
    final Future<?> reader =
        pool.submit(
            () -> {
              while (proceed.get()) {
                try (Txn<T> txn = env.txnRead()) {
                  db.get(txn, serializer.apply(50));
                }
              }
            });

    for (final Integer key : keys) {
      try (Txn<T> txn = env.txnWrite()) {
        db.put(txn, serializer.apply(key), serializer.apply(3));
        txn.commit();
      }
    }

    try (Txn<T> txn = env.txnRead();
        CursorIterable<T> ci = db.iterate(txn)) {
      final Iterator<KeyVal<T>> iter = ci.iterator();
      final List<Integer> result = new ArrayList<>();
      while (iter.hasNext()) {
        result.add(deserializer.applyAsInt(iter.next().key()));
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
    final byte[] dbHello = new byte[] {'h', 'e', 'l', 'l', 'o'};
    final byte[] dbWorld = new byte[] {'w', 'o', 'r', 'l', 'd'};
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
  public void listsFlags() {
    final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT, MDB_REVERSEKEY);

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final List<DbiFlags> flags = dbi.listFlags(txn);
      assertThat(flags, containsInAnyOrder(MDB_DUPSORT, MDB_REVERSEKEY));
    }
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
    try (Env<byte[]> envBa =
        create(PROXY_BA)
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
        assertThat(fromBa(txn.val()), is(5));
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
    assertThat(stat.pageSize % 4_096, is(0));
  }

  @Test(expected = MapFullException.class)
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
        .forEach(
            ignored -> {
              for (int i = 0; i < 15_000; i++) {
                db.put(bb(i), bb(i));
              }
            });
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsOpenCall() {
    env.close();
    env.openDbi(DB_1, MDB_CREATE);
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsCloseCall() {
    doEnvClosedTest(null, (db, txn) -> db.close());
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsGetCall() {
    doEnvClosedTest(
        (db, txn) -> {
          final ByteBuffer valBuf = db.get(txn, bb(1));
          assertThat(valBuf.getInt(), is(10));
        },
        (db, txn) -> db.get(txn, bb(2)));
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsPutCall() {
    doEnvClosedTest(null, (db, txn) -> db.put(bb(5), bb(50)));
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsPutWithTxnCall() {
    doEnvClosedTest(
        null,
        (db, txn) -> {
          db.put(txn, bb(5), bb(50));
        });
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsIterateCall() {
    doEnvClosedTest(null, Dbi::iterate);
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsDropCall() {
    doEnvClosedTest(null, Dbi::drop);
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsDropAndDeleteCall() {
    doEnvClosedTest(null, (db, txn) -> db.drop(txn, true));
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsOpenCursorCall() {
    doEnvClosedTest(null, Dbi::openCursor);
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsReserveCall() {
    doEnvClosedTest(null, (db, txn) -> db.reserve(txn, bb(1), 32, MDB_NOOVERWRITE));
  }

  @Test(expected = AlreadyClosedException.class)
  public void closedEnvRejectsStatCall() {
    doEnvClosedTest(null, Dbi::stat);
  }

  private void doEnvClosedTest(
      final BiConsumer<Dbi<ByteBuffer>, Txn<ByteBuffer>> workBeforeEnvClosed,
      final BiConsumer<Dbi<ByteBuffer>, Txn<ByteBuffer>> workAfterEnvClose) {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    db.put(bb(1), bb(10));
    db.put(bb(2), bb(20));
    db.put(bb(2), bb(30));
    db.put(bb(4), bb(40));

    try (Txn<ByteBuffer> txn = env.txnWrite()) {

      if (workBeforeEnvClosed != null) {
        workBeforeEnvClosed.accept(db, txn);
      }

      env.close();

      if (workAfterEnvClose != null) {
        workAfterEnvClose.accept(db, txn);
      }
    }
  }
}
