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

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_INTEGERKEY;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.KeyRange.all;
import static org.lmdbjava.KeyRange.allBackward;
import static org.lmdbjava.KeyRange.atLeast;
import static org.lmdbjava.KeyRange.atLeastBackward;
import static org.lmdbjava.KeyRange.atMost;
import static org.lmdbjava.KeyRange.atMostBackward;
import static org.lmdbjava.KeyRange.closed;
import static org.lmdbjava.KeyRange.closedBackward;
import static org.lmdbjava.KeyRange.closedOpen;
import static org.lmdbjava.KeyRange.closedOpenBackward;
import static org.lmdbjava.KeyRange.greaterThan;
import static org.lmdbjava.KeyRange.greaterThanBackward;
import static org.lmdbjava.KeyRange.lessThan;
import static org.lmdbjava.KeyRange.lessThanBackward;
import static org.lmdbjava.KeyRange.open;
import static org.lmdbjava.KeyRange.openBackward;
import static org.lmdbjava.KeyRange.openClosed;
import static org.lmdbjava.KeyRange.openClosedBackward;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.DB_2;
import static org.lmdbjava.TestUtils.DB_3;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.TestUtils.bbNative;
import static org.lmdbjava.TestUtils.getNativeInt;

import com.google.common.primitives.UnsignedBytes;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterable.KeyVal;

/** Test {@link CursorIterable} using {@link DbiFlags#MDB_INTEGERKEY} to ensure that
 * comparators work with native order integer keys. */
public final class CursorIterableIntegerKeyTest {

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<ByteBuffer> dbJavaComparator;
  private Dbi<ByteBuffer> dbLmdbComparator;
  private Dbi<ByteBuffer> dbCallbackComparator;
  private List<Dbi<ByteBuffer>> dbs = new ArrayList<>();
  private Env<ByteBuffer> env;
  private Deque<Integer> list;

  @After
  public void after() {
    env.close();
  }

  @Test
  public void allBackwardTest() {
    verify(allBackward(), 8, 6, 4, 2);
  }

  @Test
  public void allTest() {
    verify(all(), 2, 4, 6, 8);
  }

  @Test
  public void atLeastBackwardTest() {
    verify(atLeastBackward(bbNative(5)), 4, 2);
    verify(atLeastBackward(bbNative(6)), 6, 4, 2);
    verify(atLeastBackward(bbNative(9)), 8, 6, 4, 2);
  }

  @Test
  public void atLeastTest() {
    verify(atLeast(bbNative(5)), 6, 8);
    verify(atLeast(bbNative(6)), 6, 8);
  }

  @Test
  public void atMostBackwardTest() {
    verify(atMostBackward(bbNative(5)), 8, 6);
    verify(atMostBackward(bbNative(6)), 8, 6);
  }

  @Test
  public void atMostTest() {
    verify(atMost(bbNative(5)), 2, 4);
    verify(atMost(bbNative(6)), 2, 4, 6);
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    final BufferProxy<ByteBuffer> bufferProxy = ByteBufferProxy.PROXY_OPTIMAL;
    env =
        create(bufferProxy)
            .setMapSize(KIBIBYTES.toBytes(256))
            .setMaxReaders(1)
            .setMaxDbs(3)
            .open(path, POSIX_MODE, MDB_NOSUBDIR);

    // Use a java comparator for start/stop keys only
    DbiFlagSet dbiFlagSet = DbiFlagSet.of(MDB_CREATE, MDB_INTEGERKEY);

    dbJavaComparator = env.buildDbi()
        .withDbName(DB_1)
        .withIteratorComparator(bufferProxy.getComparator(dbiFlagSet))
        .withDbiFlags(dbiFlagSet)
        .open();

    // Use LMDB comparator for start/stop keys
    dbLmdbComparator = env.buildDbi()
        .withDbName(DB_2)
        .withDefaultComparator()
        .withDbiFlags(dbiFlagSet)
        .open();

    // Use a java comparator for start/stop keys and as a callback comparaotr
    dbCallbackComparator = env.buildDbi()
        .withDbName(DB_3)
        .withCallbackComparator(bufferProxy.getComparator(dbiFlagSet))
        .withDbiFlags(dbiFlagSet)
        .open();

    populateList();

    populateDatabase(dbJavaComparator);
    populateDatabase(dbLmdbComparator);
    populateDatabase(dbCallbackComparator);

    dbs.add(dbJavaComparator);
    dbs.add(dbLmdbComparator);
    dbs.add(dbCallbackComparator);
  }

  private void populateList() {
    list = new LinkedList<>();
    list.addAll(asList(2, 3, 4, 5, 6, 7, 8, 9));
  }

  private void populateDatabase(final Dbi<ByteBuffer> dbi) {
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      c.put(bbNative(2), bb(3), MDB_NOOVERWRITE);
      c.put(bbNative(4), bb(5));
      c.put(bbNative(6), bb(7));
      c.put(bbNative(8), bb(9));
      txn.commit();
    }
  }

  @Test
  public void closedBackwardTest() {
    verify(closedBackward(bbNative(7), bbNative(3)), 6, 4);
    verify(closedBackward(bbNative(6), bbNative(2)), 6, 4, 2);
    verify(closedBackward(bbNative(9), bbNative(3)), 8, 6, 4);
  }

  @Test
  public void closedOpenBackwardTest() {
    verify(closedOpenBackward(bbNative(8), bbNative(3)), 8, 6, 4);
    verify(closedOpenBackward(bbNative(7), bbNative(2)), 6, 4);
    verify(closedOpenBackward(bbNative(9), bbNative(3)), 8, 6, 4);
  }

  @Test
  public void closedOpenTest() {
    verify(closedOpen(bbNative(3), bbNative(8)), 4, 6);
    verify(closedOpen(bbNative(2), bbNative(6)), 2, 4);
  }

  @Test
  public void closedTest() {
    verify(closed(bbNative(3), bbNative(7)), 4, 6);
    verify(closed(bbNative(2), bbNative(6)), 2, 4, 6);
    verify(closed(bbNative(1), bbNative(7)), 2, 4, 6);
  }

  @Test
  public void greaterThanBackwardTest() {
    verify(greaterThanBackward(bbNative(6)), 4, 2);
    verify(greaterThanBackward(bbNative(7)), 6, 4, 2);
    verify(greaterThanBackward(bbNative(9)), 8, 6, 4, 2);
  }

  @Test
  public void greaterThanTest() {
    verify(greaterThan(bbNative(4)), 6, 8);
    verify(greaterThan(bbNative(3)), 4, 6, 8);
  }

  @Test(expected = IllegalStateException.class)
  public void iterableOnlyReturnedOnce() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        c.iterator(); // ok
        c.iterator(); // fails
      }
    }
  }

  @Test
  public void iterate() {
    for (final Dbi<ByteBuffer> db : dbs) {
      populateList();
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {

        int cnt = 0;
        for (final KeyVal<ByteBuffer> kv : c) {
          assertThat(getNativeInt(kv.key()), is(list.pollFirst()));
          assertThat(kv.val().getInt(), is(list.pollFirst()));
        }
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void iteratorOnlyReturnedOnce() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        c.iterator(); // ok
        c.iterator(); // fails
      }
    }
  }

  @Test
  public void lessThanBackwardTest() {
    verify(lessThanBackward(bbNative(5)), 8, 6);
    verify(lessThanBackward(bbNative(2)), 8, 6, 4);
  }

  @Test
  public void lessThanTest() {
    verify(lessThan(bbNative(5)), 2, 4);
    verify(lessThan(bbNative(8)), 2, 4, 6);
  }

  @Test(expected = NoSuchElementException.class)
  public void nextThrowsNoSuchElementExceptionIfNoMoreElements() {
    for (final Dbi<ByteBuffer> db : dbs) {
      populateList();
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        final Iterator<KeyVal<ByteBuffer>> i = c.iterator();
        while (i.hasNext()) {
          final KeyVal<ByteBuffer> kv = i.next();
          assertThat(TestUtils.getNativeInt(kv.key()), is(list.pollFirst()));
          assertThat(kv.val().getInt(), is(list.pollFirst()));
        }
        assertThat(i.hasNext(), is(false));
        i.next();
      }
    }
  }

  @Test
  public void openBackwardTest() {
    verify(openBackward(bbNative(7), bbNative(2)), 6, 4);
    verify(openBackward(bbNative(8), bbNative(1)), 6, 4, 2);
    verify(openBackward(bbNative(9), bbNative(4)), 8, 6);
  }

  @Test
  public void openClosedBackwardTest() {
    verify(openClosedBackward(bbNative(7), bbNative(2)), 6, 4, 2);
    verify(openClosedBackward(bbNative(8), bbNative(4)), 6, 4);
    verify(openClosedBackward(bbNative(9), bbNative(4)), 8, 6, 4);
  }

  @Test
  public void openClosedBackwardTestWithGuava() {
    final Comparator<byte[]> guava = UnsignedBytes.lexicographicalComparator();
    final Comparator<ByteBuffer> comparator =
        (bb1, bb2) -> {
          final byte[] array1 = new byte[bb1.remaining()];
          final byte[] array2 = new byte[bb2.remaining()];
          bb1.mark();
          bb2.mark();
          bb1.get(array1);
          bb2.get(array2);
          bb1.reset();
          bb2.reset();
          return guava.compare(array1, array2);
        };
    final Dbi<ByteBuffer> guavaDbi = env.openDbi(DB_1, comparator, MDB_CREATE);
    populateDatabase(guavaDbi);
    verify(openClosedBackward(bbNative(7), bbNative(2)), guavaDbi, 6, 4, 2);
    verify(openClosedBackward(bbNative(8), bbNative(4)), guavaDbi, 6, 4);
  }

  @Test
  public void openClosedTest() {
    verify(openClosed(bbNative(3), bbNative(8)), 4, 6, 8);
    verify(openClosed(bbNative(2), bbNative(6)), 4, 6);
  }

  @Test
  public void openTest() {
    verify(open(bbNative(3), bbNative(7)), 4, 6);
    verify(open(bbNative(2), bbNative(8)), 4, 6);
  }

  @Test
  public void removeOddElements() {
    for (final Dbi<ByteBuffer> db : dbs) {
      verify(db, all(), 2, 4, 6, 8);
      int idx = -1;
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn)) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();
          while (c.hasNext()) {
            c.next();
            idx++;
            if (idx % 2 == 0) {
              c.remove();
            }
          }
        }
        txn.commit();
      }
      verify(db, all(), 4, 8);
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void nextWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.next();
        }
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void removeWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          final KeyVal<ByteBuffer> keyVal = c.next();
          assertThat(keyVal, Matchers.notNullValue());

          env.close();
          c.remove();
        }
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void hasNextWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.hasNext();
        }
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void forEachRemainingWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.forEachRemaining(keyVal -> {});
        }
      }
    }
  }

//  @Test
//  public void testSignedVsUnsigned() {
//    final ByteBuffer val1 = bbNative(1);
//    final ByteBuffer val2 = bbNative(2);
//    final ByteBuffer val110 = bbNative(110);
//    final ByteBuffer val111 = bbNative(111);
//    final ByteBuffer val150 = bbNative(150);
//
//    final BufferProxy<ByteBuffer> bufferProxy = ByteBufferProxy.PROXY_OPTIMAL;
//    final Comparator<ByteBuffer> unsignedComparator = bufferProxy.getUnsignedComparator();
//    final Comparator<ByteBuffer> signedComparator = bufferProxy.getSignedComparator();
//
//    // Compare the same
//    assertThat(
//        unsignedComparator.compare(val1, val2), Matchers.is(signedComparator.compare(val1, val2)));
//
//    // Compare differently
//    assertThat(
//        unsignedComparator.compare(val110, val150),
//        Matchers.not(signedComparator.compare(val110, val150)));
//
//    // Compare differently
//    assertThat(
//        unsignedComparator.compare(val111, val150),
//        Matchers.not(signedComparator.compare(val111, val150)));
//
//    // This will fail if the db is using a signed comparator for the start/stop keys
//    for (final Dbi<ByteBuffer> db : dbs) {
//      db.put(val110, val110);
//      db.put(val150, val150);
//
//      final ByteBuffer startKeyBuf = val111;
//      KeyRange<ByteBuffer> keyRange = KeyRange.atLeastBackward(startKeyBuf);
//
//      try (Txn<ByteBuffer> txn = env.txnRead();
//          CursorIterable<ByteBuffer> c = db.iterate(txn, keyRange)) {
//        for (final KeyVal<ByteBuffer> kv : c) {
//          final int key = getNativeInt(kv.key());
//          final int val = kv.val().getInt();
//          //          System.out.println("key: " + key + " val: " + val);
//          assertThat(key, is(110));
//          break;
//        }
//      }
//    }
//  }

  private void verify(final KeyRange<ByteBuffer> range, final int... expected) {
    // Verify using all comparator types
    for (final Dbi<ByteBuffer> db : dbs) {
      verify(range, db, expected);
    }
  }

  private void verify(
      final Dbi<ByteBuffer> dbi, final KeyRange<ByteBuffer> range, final int... expected) {
    verify(range, dbi, expected);
  }

  private void verify(
      final KeyRange<ByteBuffer> range, final Dbi<ByteBuffer> dbi, final int... expected) {

    final List<Integer> results = new ArrayList<>();

    try (Txn<ByteBuffer> txn = env.txnRead();
        CursorIterable<ByteBuffer> c = dbi.iterate(txn, range)) {
      for (final KeyVal<ByteBuffer> kv : c) {
        final int key = kv.key().order(ByteOrder.nativeOrder()).getInt();
        final int val = kv.val().getInt();
        results.add(key);
        assertThat(val, is(key + 1));
      }
    }

    assertThat(results, hasSize(expected.length));
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx), is(expected[idx]));
    }
  }
}
