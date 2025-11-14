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

import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.TestUtils.bbNative;
import static org.lmdbjava.TestUtils.parseInt;
import static org.lmdbjava.TestUtils.parseLong;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.lmdbjava.CursorIterable.KeyVal;

/**
 * Test {@link CursorIterable}.
 */
public final class CursorIterableRangeTest {

  private static final DbiFlagSet FLAGSET_DUPSORT =
      DbiFlagSet.of(DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT);
  private static final DbiFlagSet FLAGSET_INTEGERKEY =
      DbiFlagSet.of(DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
  private static final DbiFlagSet FLAGSET_INTEGERKEY_DUPSORT =
      DbiFlagSet.of(DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT);

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testSignedComparator.csv")
  void testSignedComparator(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
        builder -> builder.withCallbackComparator(ignored -> ByteBuffer::compareTo),
        createBasicDBPopulator(),
        DbiFlags.MDB_CREATE,
        keyType,
        startKey,
        stopKey,
        expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparator.csv")
  void testUnsignedComparator(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(createBasicDBPopulator(), DbiFlags.MDB_CREATE, keyType, startKey, stopKey, expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparator.csv")
  void testUnsignedComparator_Iterator(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(createBasicDBPopulator(), DbiFlags.MDB_CREATE, keyType, startKey, stopKey, expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparator.csv")
  void testUnsignedComparator_Callback(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(createBasicDBPopulator(), DbiFlags.MDB_CREATE, keyType, startKey, stopKey, expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testSignedComparatorDupsort.csv")
  void testSignedComparatorDupsort(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
        builder -> builder.withCallbackComparator(ignored -> ByteBuffer::compareTo),
        createMultiDBPopulator(2),
        FLAGSET_DUPSORT,
        keyType,
        startKey,
        stopKey,
        expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparatorDupsort.csv")
  void testUnsignedComparatorDupsort(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(createMultiDBPopulator(2), FLAGSET_DUPSORT, keyType, startKey, stopKey, expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testIntegerKey.csv")
  void testIntegerKey(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
        createIntegerDBPopulator(),
        FLAGSET_INTEGERKEY,
        keyType,
        startKey,
        stopKey,
        expectedKV,
        Integer.BYTES,
        ByteOrder.nativeOrder());
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparatorDupsort.csv")
  void testIntegerKeyDupSort(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
        createMultiIntegerDBPopulator(2),
        FLAGSET_INTEGERKEY_DUPSORT,
        keyType,
        startKey,
        stopKey,
        expectedKV,
        Integer.BYTES,
        ByteOrder.nativeOrder());
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testLongKey.csv")
  void testLongKey(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
        createLongDBPopulator(),
        FLAGSET_INTEGERKEY,
        keyType,
        startKey,
        stopKey,
        expectedKV,
        Long.BYTES,
        ByteOrder.nativeOrder());
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparatorDupsort.csv")
  void testLongKeyDupSort(
      final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
        createMultiLongDBPopulator(2),
        FLAGSET_INTEGERKEY_DUPSORT,
        keyType,
        startKey,
        stopKey,
        expectedKV,
        Long.BYTES,
        ByteOrder.nativeOrder());
  }

  private void testCSV(
      final Function<DbiBuilder.Stage2<ByteBuffer>, DbiBuilder.Stage3<ByteBuffer>> comparatorFunc,
      final BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> dbPopulator,
      final DbiFlagSet dbiFlags,
      final String keyType,
      final String startKey,
      final String stopKey,
      final String expectedKV) {
    testCSV(
        comparatorFunc,
        dbPopulator,
        dbiFlags,
        keyType,
        startKey,
        stopKey,
        expectedKV,
        Integer.BYTES,
        ByteOrder.BIG_ENDIAN);
  }

  private void testCSV(
      final BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> dbPopulator,
      final DbiFlagSet dbiFlags,
      final String keyType,
      final String startKey,
      final String stopKey,
      final String expectedKV) {
    testCSV(
        dbPopulator,
        dbiFlags,
        keyType,
        startKey,
        stopKey,
        expectedKV,
        Integer.BYTES,
        ByteOrder.BIG_ENDIAN);
  }

  private void testCSV(
      final BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> dbPopulator,
      final DbiFlagSet dbiFlags,
      final String keyType,
      final String startKey,
      final String stopKey,
      final String expectedKV,
      final int keyLen,
      final ByteOrder byteOrder) {

    // We want to assert that the behaviour of all 4 comparator functions
    // is identical.

    final List<Function<DbiBuilder.Stage2<ByteBuffer>, DbiBuilder.Stage3<ByteBuffer>>> comparatorFuncs =
        new ArrayList<>();

    // First test with our default iterator comparator
    comparatorFuncs.add(DbiBuilder.Stage2::withDefaultComparator);
    // Now test with mdp_cmp doing all comparisons, should be the same
    comparatorFuncs.add(DbiBuilder.Stage2::withNativeComparator);
    // Now test with the java callback comparator doing all the work
    comparatorFuncs.add(byteBufferStage2 ->
        byteBufferStage2.withCallbackComparator(ByteBufferProxy.PROXY_OPTIMAL::getComparator));
    // Now test with the java comparator for iteration only
    comparatorFuncs.add(byteBufferStage2 ->
        byteBufferStage2.withIteratorComparator(ByteBufferProxy.PROXY_OPTIMAL::getComparator));

    for (Function<DbiBuilder.Stage2<ByteBuffer>, DbiBuilder.Stage3<ByteBuffer>> comparatorFunc : comparatorFuncs) {
      testCSV(
          comparatorFunc,
          dbPopulator,
          dbiFlags,
          keyType,
          startKey,
          stopKey,
          expectedKV,
          keyLen,
          byteOrder);
    }
  }

  private void testCSV(
      final Function<DbiBuilder.Stage2<ByteBuffer>, DbiBuilder.Stage3<ByteBuffer>> comparatorFunc,
      final BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> dbPopulator,
      final DbiFlagSet dbiFlags,
      final String keyType,
      final String startKey,
      final String stopKey,
      final String expectedKV,
      final int keyLen,
      final ByteOrder byteOrder) {
    try (final TempDir tempDir = new TempDir()) {
      final Path file = tempDir.createTempFile();
      try (final Env<ByteBuffer> env =
               create()
                   .setMapSize(256, ByteUnit.KIBIBYTES)
                   .setMaxReaders(1)
                   .setMaxDbs(1)
                   .setEnvFlags(EnvFlags.MDB_NOSUBDIR)
                   .open(file)) {

        final DbiBuilder.Stage2<ByteBuffer> builderStage2 = env.createDbi().setDbName(DB_1);
        final DbiBuilder.Stage3<ByteBuffer> builderStage3 = comparatorFunc.apply(builderStage2);
        final Dbi<ByteBuffer> dbi = builderStage3.setDbiFlags(dbiFlags).open();

        dbPopulator.accept(env, dbi);
        try (final Writer writer = new StringWriter()) {
          final KeyRangeType keyRangeType = KeyRangeType.valueOf(keyType.trim());
          ByteBuffer start = parseKey(startKey, keyLen, byteOrder);
          ByteBuffer stop = parseKey(stopKey, keyLen, byteOrder);

          final KeyRange<ByteBuffer> keyRange = new KeyRange<>(keyRangeType, start, stop);
          try (Txn<ByteBuffer> txn = env.txnRead();
               CursorIterable<ByteBuffer> c = dbi.iterate(txn, keyRange)) {
            for (final KeyVal<ByteBuffer> kv : c) {
              final long key = getLong(kv.key(), byteOrder);
              final long val = getLong(kv.val(), ByteOrder.BIG_ENDIAN);
              writer.append("[");
              writer.append(String.valueOf(key));
              writer.append(" ");
              writer.append(String.valueOf(val));
              writer.append("]");
            }
          }
          assertThat(writer.toString()).isEqualTo(expectedKV == null ? "" : expectedKV);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  private ByteBuffer parseKey(final String key, final int keyLen, final ByteOrder byteOrder) {
    if (key != null) {
      if (ByteOrder.nativeOrder().equals(byteOrder)) {
        if (keyLen == Integer.BYTES) {
          return bbNative(parseInt(key));
        } else {
          return bbNative(parseLong(key));
        }
      } else {
        if (keyLen == Integer.BYTES) {
          return bb(parseInt(key));
        } else {
          return bb(parseLong(key));
        }
      }
    }
    return null;
  }

  private long getLong(final ByteBuffer byteBuffer, final ByteOrder byteOrder) {
    byteBuffer.order(byteOrder);
    if (byteBuffer.remaining() == Integer.BYTES) {
      return byteBuffer.getInt();
    } else {
      return byteBuffer.getLong();
    }
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createBasicDBPopulator() {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        c.put(bb(0), bb(1));
        c.put(bb(2), bb(3));
        c.put(bb(4), bb(5));
        c.put(bb(6), bb(7));
        c.put(bb(8), bb(9));
        c.put(bb(-2), bb(-1));
        txn.commit();
      }
    };
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createMultiDBPopulator(final int copies) {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        for (int i = 0; i < copies; i++) {
          c.put(bb(0), bb(1 + i));
          c.put(bb(2), bb(3 + i));
          c.put(bb(4), bb(5 + i));
          c.put(bb(6), bb(7 + i));
          c.put(bb(8), bb(9 + i));
          c.put(bb(-2), bb(-1 + i));
        }
        txn.commit();
      }
    };
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createMultiIntegerDBPopulator(
      final int copies) {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        for (int i = 0; i < copies; i++) {
          c.put(bbNative(0), bb(1 + i));
          c.put(bbNative(2), bb(3 + i));
          c.put(bbNative(4), bb(5 + i));
          c.put(bbNative(6), bb(7 + i));
          c.put(bbNative(8), bb(9 + i));
          c.put(bbNative(-2), bb(-1 + i));
        }
        txn.commit();
      }
    };
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createMultiLongDBPopulator(
      final int copies) {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        for (int i = 0; i < copies; i++) {
          c.put(bbNative(0L), bb(1 + i));
          c.put(bbNative(2L), bb(3 + i));
          c.put(bbNative(4L), bb(5 + i));
          c.put(bbNative(6L), bb(7 + i));
          c.put(bbNative(8L), bb(9 + i));
          c.put(bbNative(-2L), bb(-1 + i));
        }
        txn.commit();
      }
    };
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createIntegerDBPopulator() {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        c.put(bbNative(0), bb(1));
        c.put(bbNative(1000), bb(2));
        c.put(bbNative(1000000), bb(3));
        c.put(bbNative(-1000000), bb(4));
        c.put(bbNative(-1000), bb(5));
        txn.commit();
      }
    };
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createLongDBPopulator() {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        c.put(bbNative(0L), bb(1));
        c.put(bbNative(1000L), bb(2));
        c.put(bbNative(1000000L), bb(3));
        c.put(bbNative(-1000000L), bb(4));
        c.put(bbNative(-1000L), bb(5));
        txn.commit();
      }
    };
  }
}
