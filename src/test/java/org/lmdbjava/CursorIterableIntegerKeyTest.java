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
import static org.assertj.core.api.Assertions.assertThat;
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
import static org.lmdbjava.TestUtils.DB_4;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.TestUtils.bbNative;
import static org.lmdbjava.TestUtils.getNativeInt;
import static org.lmdbjava.TestUtils.getNativeIntOrLong;
import static org.lmdbjava.TestUtils.getNativeLong;
import static org.lmdbjava.TestUtils.getString;

import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.lmdbjava.CursorIterable.KeyVal;

/**
 * Test {@link CursorIterable} using {@link DbiFlags#MDB_INTEGERKEY} to ensure that
 * comparators work with native order integer keys.
 */
@ParameterizedClass(name = "{index}: dbi: {0}")
@ArgumentsSource(CursorIterableIntegerKeyTest.MyArgumentProvider.class)
public final class CursorIterableIntegerKeyTest {

  private static final DbiFlagSet DBI_FLAGS = DbiFlagSet.of(MDB_CREATE, MDB_INTEGERKEY);
  private static final BufferProxy<ByteBuffer> BUFFER_PROXY = ByteBufferProxy.PROXY_OPTIMAL;

  private Path file;
  private Env<ByteBuffer> env;
  private Deque<Integer> list;

  @Parameter
  public DbiFactory dbiFactory;


  @BeforeEach
  public void before() throws IOException {
    file = FileUtil.createTempFile();
    final BufferProxy<ByteBuffer> bufferProxy = ByteBufferProxy.PROXY_OPTIMAL;
    env = create(bufferProxy)
        .setMapSize(KIBIBYTES.toBytes(256))
        .setMaxReaders(1)
        .setMaxDbs(3)
        .open(file.toFile(), POSIX_MODE, MDB_NOSUBDIR);

    populateTestDataList();
  }

  @AfterEach
  public void after() {
    env.close();
    FileUtil.delete(file);
  }

  @Test
  public void testNumericOrderLong() {
    final Dbi<ByteBuffer> dbi = dbiFactory.factory.apply(env);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      long i = 1;
      while (true) {
        System.out.println("putting " + i);
        c.put(bbNative(i), bb(i + "-long"));
        final long i2 = i * 10;
        if (i2 < i) {
          // Overflowed
          break;
        }
        i = i2;
      }
      txn.commit();
    }

    final List<Map.Entry<Long, String>> entries = new ArrayList<>();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> iterable = dbi.iterate(txn)) {
        for (KeyVal<ByteBuffer> keyVal : iterable) {
          assertThat(keyVal.key().remaining()).isEqualTo(Long.BYTES);
          final String val = getString(keyVal.val());
          final long key = getNativeLong(keyVal.key());
          entries.add(new AbstractMap.SimpleEntry<>(key, val));
//          System.out.println(val);
        }
      }
    }

    final List<Long> dbKeys = entries.stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    final List<Long> dbKeysSorted = entries.stream()
        .map(Map.Entry::getKey)
        .sorted()
        .collect(Collectors.toList());
    for (int i = 0; i < dbKeys.size(); i++) {
      final long dbKey1 = dbKeys.get(i);
      final long dbKey2 = dbKeysSorted.get(i);
      assertThat(dbKey1).isEqualTo(dbKey2);
    }
  }

  @Test
  public void testNumericOrderInt() {
    final Dbi<ByteBuffer> dbi = dbiFactory.factory.apply(env);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      int i = 1;
      while (true) {
        System.out.println("putting " + i);
        c.put(bbNative(i), bb(i + "-int"));
        final int i2 = i * 10;
        if (i2 < i) {
          // Overflowed
          break;
        }
        i = i2;
      }
      txn.commit();
    }

    final List<Map.Entry<Integer, String>> entries = new ArrayList<>();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> iterable = dbi.iterate(txn)) {
        for (KeyVal<ByteBuffer> keyVal : iterable) {
          assertThat(keyVal.key().remaining()).isEqualTo(Integer.BYTES);
          final String val = getString(keyVal.val());
          final int key = TestUtils.getNativeInt(keyVal.key());
          entries.add(new AbstractMap.SimpleEntry<>(key, val));
//          System.out.println(val);
        }
      }
    }

    final List<Integer> dbKeys = entries.stream()
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    final List<Integer> dbKeysSorted = entries.stream()
        .map(Map.Entry::getKey)
        .sorted()
        .collect(Collectors.toList());
    for (int i = 0; i < dbKeys.size(); i++) {
      final long dbKey1 = dbKeys.get(i);
      final long dbKey2 = dbKeysSorted.get(i);
      assertThat(dbKey1).isEqualTo(dbKey2);
    }
  }

  @Test
  public void testIntegerKeyKeySize() {
    final Dbi<ByteBuffer> db = dbiFactory.factory.apply(env);
    long maxIntAsLong = Integer.MAX_VALUE;

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      System.out.println("Flags: " + db.listFlags(txn));
      int val = 0;
      db.put(txn, bbNative(0L), bb("val_" + ++val));
      db.put(txn, bbNative(10L), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong - 1_111_111_111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong - 111_111_111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong - 111_111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong - 111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong + 111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong + 111_111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong + 111_111_111), bb("val_" + ++val));
      db.put(txn, bbNative(maxIntAsLong + 1_111_111_111), bb("val_" + ++val));
      db.put(txn, bbNative(Long.MAX_VALUE), bb("val_" + ++val));
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> iterable = db.iterate(txn)) {
        for (KeyVal<ByteBuffer> keyVal : iterable) {
          final String val = getString(keyVal.val());
          final long key = getNativeLong(keyVal.key());
          final int remaining = keyVal.key().remaining();
          System.out.println("key: " + key + ", val: " + val + ", remaining: " + remaining);
        }
      }
    }

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

  private void populateTestDataList() {
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

  public void iterableOnlyReturnedOnce() {
    Assertions.assertThatThrownBy(() -> {
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnRead();
           CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        c.iterator(); // ok
        c.iterator(); // fails
      }
    }).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void iterate() {
    populateTestDataList();
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {

      for (final KeyVal<ByteBuffer> kv : c) {
        assertThat(getNativeInt(kv.key())).isEqualTo(list.pollFirst());
        assertThat(kv.val().getInt()).isEqualTo(list.pollFirst());
      }
    }
  }

  public void iteratorOnlyReturnedOnce() {
    Assertions.assertThatThrownBy(() -> {
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnRead();
           CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        c.iterator(); // ok
        c.iterator(); // fails
      }
    }).isInstanceOf(IllegalStateException.class);
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

  public void nextThrowsNoSuchElementExceptionIfNoMoreElements() {
    Assertions.assertThatThrownBy(() -> {
      populateTestDataList();
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnRead();
           CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        final Iterator<KeyVal<ByteBuffer>> i = c.iterator();
        while (i.hasNext()) {
          final KeyVal<ByteBuffer> kv = i.next();
          assertThat(getNativeInt(kv.key())).isEqualTo(list.pollFirst());
          assertThat(kv.val().getInt()).isEqualTo(list.pollFirst());
        }
        assertThat(i.hasNext()).isEqualTo(false);
        i.next();
      }
    }).isInstanceOf(NoSuchElementException.class);
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
    final Dbi<ByteBuffer> db = getDb();
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

  public void nextWithClosedEnvTest() {
    Assertions.assertThatThrownBy(() -> {
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.next();
        }
      }
    }).isInstanceOf(Env.AlreadyClosedException.class);
  }

  public void removeWithClosedEnvTest() {
    Assertions.assertThatThrownBy(() -> {
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          final KeyVal<ByteBuffer> keyVal = c.next();
          assertThat(keyVal).isNotNull();

          env.close();
          c.remove();
        }
      }
    }).isInstanceOf(Env.AlreadyClosedException.class);
  }

  public void hasNextWithClosedEnvTest() {
    Assertions.assertThatThrownBy(() -> {
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.hasNext();
        }
      }
    }).isInstanceOf(Env.AlreadyClosedException.class);
  }

  public void forEachRemainingWithClosedEnvTest() {
    Assertions.assertThatThrownBy(() -> {
      final Dbi<ByteBuffer> db = getDb();
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.forEachRemaining(keyVal -> {
          });
        }
      }
    }).isInstanceOf(Env.AlreadyClosedException.class);
  }

  private void verify(final KeyRange<ByteBuffer> range, final int... expected) {
    // Verify using all comparator types
    final Dbi<ByteBuffer> db = getDb();
    verify(range, db, expected);
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
        assertThat(val).isEqualTo(key + 1);
      }
    }

    assertThat(results).hasSize(expected.length);
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx)).isEqualTo(expected[idx]);
    }
  }

  private Dbi<ByteBuffer> getDb() {
    final Dbi<ByteBuffer> dbi = dbiFactory.factory.apply(env);
    populateDatabase(dbi);
    return dbi;
  }


  // --------------------------------------------------------------------------------


  private static class DbiFactory {
    private final String name;
    private final Function<Env<ByteBuffer>, Dbi<ByteBuffer>> factory;

    private DbiFactory(String name, Function<Env<ByteBuffer>, Dbi<ByteBuffer>> factory) {
      this.name = name;
      this.factory = factory;
    }

    @Override
    public String toString() {
      return name;
    }
  }


  // --------------------------------------------------------------------------------


  static class MyArgumentProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ParameterDeclarations parameters,
                                                        ExtensionContext context) throws Exception {
      final DbiFactory defaultComparatorDb = new DbiFactory("defaultComparator", env ->
          env.buildDbi()
              .withDbName(DB_1)
              .withDefaultComparator()
              .withDbiFlags(DBI_FLAGS)
              .open());
      final DbiFactory nativeComparatorDb = new DbiFactory("nativeComparator", env ->
          env.buildDbi()
              .withDbName(DB_2)
              .withNativeComparator()
              .withDbiFlags(DBI_FLAGS)
              .open());
      final Comparator<ByteBuffer> comparator = buildComparator();

      final DbiFactory callbackComparatorDb = new DbiFactory("callbackComparator", env ->
          env.buildDbi()
              .withDbName(DB_3)
              .withCallbackComparator(comparator)
              .withDbiFlags(DBI_FLAGS)
              .open());
      final DbiFactory iteratorComparatorDb = new DbiFactory("iteratorComparator", env ->
          env.buildDbi()
              .withDbName(DB_4)
              .withIteratorComparator(comparator)
              .withDbiFlags(DBI_FLAGS)
              .open());
      return Stream.of(
              defaultComparatorDb,
              nativeComparatorDb,
              callbackComparatorDb,
              iteratorComparatorDb)
          .map(Arguments::of);
    }

    private static Comparator<ByteBuffer> buildComparator() {
      final Comparator<ByteBuffer> baseComparator = BUFFER_PROXY.getComparator(DBI_FLAGS);
      return (o1, o2) -> {
        if (o1.remaining() != o2.remaining()) {
          // Make sure LMDB is always giving us consistent key lengths.
          Assertions.fail("o1: " + o1 + " " + getNativeIntOrLong(o1)
              + ", o2: " + o2 + " " + getNativeIntOrLong(o2));
        }
        return baseComparator.compare(o1, o2);
      };
    }
  }
}
