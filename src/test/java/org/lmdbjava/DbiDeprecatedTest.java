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

import static java.lang.Long.MAX_VALUE;
import static java.lang.System.getProperty;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteUnit.MEBIBYTES;
import static org.lmdbjava.DbiFlags.*;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.KeyRange.atMost;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi.DbFullException;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.LmdbNativeException.ConstantDerivedException;

/**
 * Tests all the deprecated methods in {@link Dbi}. Essentially a duplicate of {@link DbiTest}. When
 * all the deprecated methods are deleted we can delete this test class.
 *
 * @deprecated Tests all the deprecated methods in {@link Dbi}.
 */
@Deprecated
public class DbiDeprecatedTest {

  private TempDir tempDir;
  private Env<ByteBuffer> env;
  private TempDir tempDirBa;
  private Env<byte[]> envBa;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
    final Path file = tempDir.createTempFile();
    env =
        create()
            .setMapSize(MEBIBYTES.toBytes(64))
            .setMaxReaders(2)
            .setMaxDbs(2)
            .open(file.toFile(), MDB_NOSUBDIR);
    tempDirBa = new TempDir();
    final Path fileBa = tempDirBa.createTempFile();
    envBa =
        create(PROXY_BA)
            .setMapSize(MEBIBYTES.toBytes(64))
            .setMaxReaders(2)
            .setMaxDbs(2)
            .open(fileBa.toFile(), MDB_NOSUBDIR);
  }

  @AfterEach
  void afterEach() {
    env.close();
    envBa.close();
    tempDir.cleanup();
    tempDirBa.cleanup();
  }

  @Test
  void close() {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
              db.put(bb(1), bb(42));
              db.close();
              db.put(bb(2), bb(42)); // error
            })
        .isInstanceOf(ConstantDerivedException.class);
  }

  @Test
  void customComparator() {
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
  void customComparatorByteArray() {
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
      assertThat(db.put(txn, serializer.apply(2), serializer.apply(3))).isTrue();
      assertThat(db.put(txn, serializer.apply(4), serializer.apply(6))).isTrue();
      assertThat(db.put(txn, serializer.apply(6), serializer.apply(7))).isTrue();
      assertThat(db.put(txn, serializer.apply(8), serializer.apply(7))).isTrue();
      txn.commit();
    }
    try (Txn<T> txn = env.txnRead();
        CursorIterable<T> ci = db.iterate(txn, atMost(serializer.apply(4)))) {
      final Iterator<KeyVal<T>> iter = ci.iterator();
      assertThat(deserializer.applyAsInt(iter.next().key())).isEqualTo(8);
      assertThat(deserializer.applyAsInt(iter.next().key())).isEqualTo(6);
      assertThat(deserializer.applyAsInt(iter.next().key())).isEqualTo(4);
    }
  }

  @Test
  void dbOpenMaxDatabases() {
    assertThatThrownBy(
            () -> {
              env.openDbi("db1 is OK", MDB_CREATE);
              env.openDbi("db2 is OK", MDB_CREATE);
              env.openDbi("db3 fails", MDB_CREATE);
            })
        .isInstanceOf(DbFullException.class);
  }

  @Test
  void dbiWithComparatorThreadSafety() {
    doDbiWithComparatorThreadSafety(
        env, PROXY_OPTIMAL::getComparator, TestUtils::bb, ByteBuffer::getInt);
  }

  @Test
  void dbiWithComparatorThreadSafetyByteArray() {
    doDbiWithComparatorThreadSafety(
        envBa, PROXY_BA::getComparator, TestUtils::ba, TestUtils::fromBa);
  }

  private <T> void doDbiWithComparatorThreadSafety(
      Env<T> env,
      Function<DbiFlagSet, Comparator<T>> comparator,
      IntFunction<T> serializer,
      ToIntFunction<T> deserializer) {
    final DbiFlags[] flags = new DbiFlags[] {MDB_CREATE, MDB_INTEGERKEY};
    final Comparator<T> c = comparator.apply(DbiFlagSet.of(flags));
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

      assertThat(result).contains(keys.toArray(new Integer[0]));
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
  void drop() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(1), bb(42));
      db.put(txn, bb(2), bb(42));
      assertThat(db.get(txn, bb(1))).isNotNull();
      assertThat(db.get(txn, bb(2))).isNotNull();
      db.drop(txn);
      assertThat(db.get(txn, bb(1))).isNull(); // data gone
      assertThat(db.get(txn, bb(2))).isNull();
      db.put(txn, bb(1), bb(42)); // ensure DB still works
      db.put(txn, bb(2), bb(42));
      assertThat(db.get(txn, bb(1))).isNotNull();
      assertThat(db.get(txn, bb(2))).isNotNull();
    }
  }

  @Test
  void dropAndDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    final Dbi<ByteBuffer> nameDb = env.openDbi((byte[]) null);
    final byte[] dbNameBytes = DB_1.getBytes(UTF_8);
    final ByteBuffer dbNameBuffer = allocateDirect(dbNameBytes.length);
    dbNameBuffer.put(dbNameBytes).flip();

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(nameDb.get(txn, dbNameBuffer)).isNotNull();
      db.drop(txn, true);
      assertThat(nameDb.get(txn, dbNameBuffer)).isNull();
      txn.commit();
    }
  }

  @Test
  void dropAndDeleteAnonymousDb() {
    env.openDbi(DB_1, MDB_CREATE);
    final Dbi<ByteBuffer> nameDb = env.openDbi((byte[]) null);
    final byte[] dbNameBytes = DB_1.getBytes(UTF_8);
    final ByteBuffer dbNameBuffer = allocateDirect(dbNameBytes.length);
    dbNameBuffer.put(dbNameBytes).flip();

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(nameDb.get(txn, dbNameBuffer)).isNotNull();
      nameDb.drop(txn, true);
      assertThat(nameDb.get(txn, dbNameBuffer)).isNull();
      txn.commit();
    }

    nameDb.close(); // explicit close after drop is OK
  }

  @Test
  void getName() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    assertThat(db.getName()).isEqualTo(DB_1.getBytes(UTF_8));
  }

  @Test
  void getNamesWhenDbisPresent() {
    final byte[] dbHello = new byte[] {'h', 'e', 'l', 'l', 'o'};
    final byte[] dbWorld = new byte[] {'w', 'o', 'r', 'l', 'd'};
    env.openDbi(dbHello, MDB_CREATE);
    env.openDbi(dbWorld, MDB_CREATE);
    final List<byte[]> dbiNames = env.getDbiNames();
    assertThat(dbiNames).hasSize(2);
    assertThat(dbiNames.get(0)).isEqualTo(dbHello);
    assertThat(dbiNames.get(1)).isEqualTo(dbWorld);
  }

  @Test
  void getNamesWhenEmpty() {
    final List<byte[]> dbiNames = env.getDbiNames();
    assertThat(dbiNames).isEmpty();
  }

  @Test
  void listsFlags() {
    final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT, MDB_REVERSEKEY);

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final List<DbiFlags> flags = dbi.listFlags(txn);
      assertThat(flags).containsExactlyInAnyOrder(MDB_DUPSORT, MDB_REVERSEKEY);
    }
  }

  @Test
  void putAbortGet() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      txn.abort();
    }

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(db.get(txn, bb(5))).isNull();
    }
  }

  @Test
  void putAndGetAndDeleteWithInternalTx() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    db.put(bb(5), bb(5));
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertThat(found).isNotNull();
      assertThat(txn.val().getInt()).isEqualTo(5);
    }
    assertThat(db.delete(bb(5))).isTrue();
    assertThat(db.delete(bb(5))).isFalse();

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(db.get(txn, bb(5))).isNull();
    }
  }

  @Test
  void putCommitGet() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertThat(found).isNotNull();
      assertThat(txn.val().getInt()).isEqualTo(5);
    }
  }

  @Test
  void putCommitGetByteArray() {
    final Path file = tempDir.createTempFile();
    try (Env<byte[]> envBa =
        create(PROXY_BA)
            .setMapSize(MEBIBYTES.toBytes(64))
            .setMaxReaders(1)
            .setMaxDbs(2)
            .open(file.toFile(), MDB_NOSUBDIR)) {
      final Dbi<byte[]> db = envBa.openDbi(DB_1, MDB_CREATE);
      try (Txn<byte[]> txn = envBa.txnWrite()) {
        db.put(txn, ba(5), ba(5));
        txn.commit();
      }
      try (Txn<byte[]> txn = envBa.txnWrite()) {
        final byte[] found = db.get(txn, ba(5));
        assertThat(found).isNotNull();
        assertThat(fromBa(txn.val())).isEqualTo(5);
      }
    }
  }

  @Test
  void putDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      assertThat(db.delete(txn, bb(5))).isTrue();

      assertThat(db.get(txn, bb(5))).isNull();
      txn.abort();
    }
  }

  @Test
  void putDuplicateDelete() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      db.put(txn, bb(5), bb(5));
      db.put(txn, bb(5), bb(6));
      db.put(txn, bb(5), bb(7));
      assertThat(db.delete(txn, bb(5), bb(6))).isTrue();
      assertThat(db.delete(txn, bb(5), bb(6))).isFalse();
      assertThat(db.delete(txn, bb(5), bb(5))).isTrue();
      assertThat(db.delete(txn, bb(5), bb(5))).isFalse();

      try (Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
        final ByteBuffer key = bb(5);
        cursor.get(key, MDB_SET_KEY);
        assertThat(cursor.count()).isEqualTo(1L);
      }
      txn.abort();
    }
  }

  @Test
  void putReserve() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final ByteBuffer key = bb(5);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      assertThat(db.get(txn, key)).isNull();
      final ByteBuffer val = db.reserve(txn, key, 32, MDB_NOOVERWRITE);
      val.putLong(MAX_VALUE);
      assertThat(db.get(txn, key)).isNotNull();
      txn.commit();
    }
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = db.get(txn, key);
      assertThat(val).isNotNull();
      assertThat(val.capacity()).isEqualTo(32);
      assertThat(val.getLong()).isEqualTo(MAX_VALUE);
      assertThat(val.getLong(8)).isEqualTo(0L);
    }
  }

  @Test
  void putZeroByteValueForNonMdbDupSortDatabase() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer val = allocateDirect(0);
      db.put(txn, bb(5), val);
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer found = db.get(txn, bb(5));
      assertThat(found).isNotNull();
      assertThat(txn.val().capacity()).isEqualTo(0);
    }
  }

  @Test
  void returnValueForNoDupData() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      // ok
      assertThat(db.put(txn, bb(5), bb(6), MDB_NODUPDATA)).isTrue();
      assertThat(db.put(txn, bb(5), bb(7), MDB_NODUPDATA)).isTrue();
      assertThat(db.put(txn, bb(5), bb(6), MDB_NODUPDATA)).isFalse();
    }
  }

  @Test
  void returnValueForNoOverwrite() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      // ok
      assertThat(db.put(txn, bb(5), bb(6), MDB_NOOVERWRITE)).isTrue();
      // fails, but gets exist val
      assertThat(db.put(txn, bb(5), bb(8), MDB_NOOVERWRITE)).isFalse();
      assertThat(txn.val().getInt(0)).isEqualTo(6);
    }
  }

  @Test
  void stats() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    db.put(bb(1), bb(42));
    db.put(bb(2), bb(42));
    db.put(bb(3), bb(42));
    final Stat stat;
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      stat = db.stat(txn);
    }
    assertThat(stat).isNotNull();
    assertThat(stat.branchPages).isEqualTo(0L);
    assertThat(stat.depth).isEqualTo(1);
    assertThat(stat.entries).isEqualTo(3L);
    assertThat(stat.leafPages).isEqualTo(1L);
    assertThat(stat.overflowPages).isEqualTo(0L);
    assertThat(stat.pageSize % 4_096).isEqualTo(0);
  }

  @Test
  void testMapFullException() {
    assertThatThrownBy(
            () -> {
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
            })
        .isInstanceOf(MapFullException.class);
  }

  @Test
  void testParallelWritesStress() {
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

  @Test
  void closedEnvRejectsOpenCall() {
    assertThatThrownBy(
            () -> {
              env.close();
              env.openDbi(DB_1, MDB_CREATE);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsCloseCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, (db, txn) -> db.close());
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsGetCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(
                  (db, txn) -> {
                    final ByteBuffer valBuf = db.get(txn, bb(1));
                    assertThat(valBuf.getInt()).isEqualTo(10);
                  },
                  (db, txn) -> db.get(txn, bb(2)));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsPutCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, (db, txn) -> db.put(bb(5), bb(50)));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsPutWithTxnCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(
                  null,
                  (db, txn) -> {
                    db.put(txn, bb(5), bb(50));
                  });
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsIterateCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Dbi::iterate);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsDropCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Dbi::drop);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsDropAndDeleteCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, (db, txn) -> db.drop(txn, true));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsOpenCursorCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Dbi::openCursor);
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsReserveCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, (db, txn) -> db.reserve(txn, bb(1), 32, MDB_NOOVERWRITE));
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closedEnvRejectsStatCall() {
    assertThatThrownBy(
            () -> {
              doEnvClosedTest(null, Dbi::stat);
            })
        .isInstanceOf(AlreadyClosedException.class);
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
