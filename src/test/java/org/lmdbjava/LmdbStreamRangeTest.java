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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.lmdbjava.ByteBufferProxy.AbstractByteBufferProxy;
import org.lmdbjava.CursorIterable.KeyVal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import static java.nio.ByteBuffer.allocateDirect;
import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.DbiFlags.*;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.*;

/**
 * Test {@link CursorIterable}.
 */
public final class LmdbStreamRangeTest {

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testSignedComparator.csv")
  void testSignedComparator(
          final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
            ByteBuffer::compareTo,
            true,
            createBasicDBPopulator(),
            EnumSet.of(MDB_CREATE),
            keyType,
            startKey,
            stopKey,
            expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparator.csv")
  void testUnsignedComparator(
          final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
            AbstractByteBufferProxy::compareBuff,
            false,
            createBasicDBPopulator(),
            EnumSet.of(MDB_CREATE),
            keyType,
            startKey,
            stopKey,
            expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testSignedComparatorDupsort.csv")
  void testSignedComparatorDupsort(
          final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
            ByteBuffer::compareTo,
            true,
            createMultiDBPopulator(2),
            EnumSet.of(MDB_CREATE, MDB_DUPSORT),
            keyType,
            startKey,
            stopKey,
            expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testUnsignedComparatorDupsort.csv")
  void testUnsignedComparatorDupsort(
          final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
            AbstractByteBufferProxy::compareBuff,
            false,
            createMultiDBPopulator(2),
            EnumSet.of(MDB_CREATE, MDB_DUPSORT),
            keyType,
            startKey,
            stopKey,
            expectedKV);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testIntegerKey.csv")
  void testIntegerKey(
          final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
            AbstractByteBufferProxy::compareBuff,
            false,
            createIntegerDBPopulator(),
            EnumSet.of(MDB_CREATE, MDB_INTEGERKEY),
            keyType,
            startKey,
            stopKey,
            expectedKV,
            Integer.BYTES,
            ByteOrder.LITTLE_ENDIAN);
  }

  @ParameterizedTest(name = "{index} => {0}: ({1}, {2})")
  @CsvFileSource(resources = "/CursorIterableRangeTest/testLongKey.csv")
  void testLongKey(
          final String keyType, final String startKey, final String stopKey, final String expectedKV) {
    testCSV(
            AbstractByteBufferProxy::compareBuff,
            false,
            createLongDBPopulator(),
            EnumSet.of(MDB_CREATE, MDB_INTEGERKEY),
            keyType,
            startKey,
            stopKey,
            expectedKV,
            Long.BYTES,
            ByteOrder.LITTLE_ENDIAN);
  }

  private void testCSV(
          final Comparator<ByteBuffer> comparator,
          final boolean nativeCb,
          final BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> dbPopulator,
          final EnumSet<DbiFlags> flags,
          final String keyType,
          final String startKey,
          final String stopKey,
          final String expectedKV) {
    testCSV(
            comparator,
            nativeCb,
            dbPopulator,
            flags,
            keyType,
            startKey,
            stopKey,
            expectedKV,
            Integer.BYTES,
            ByteOrder.BIG_ENDIAN);
  }

  private void testCSV(
          final Comparator<ByteBuffer> comparator,
          final boolean nativeCb,
          final BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> dbPopulator,
          final EnumSet<DbiFlags> flags,
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
                           .setMapSize(KIBIBYTES.toBytes(256))
                           .setMaxReaders(1)
                           .setMaxDbs(1)
                           .open(file.toFile(), POSIX_MODE, MDB_NOSUBDIR)) {
        final Dbi<ByteBuffer> dbi =
                env.openDbi(DB_1, comparator, nativeCb, flags.toArray(new DbiFlags[0]));
        dbPopulator.accept(env, dbi);
        try (final Writer writer = new StringWriter()) {
          final KeyRangeType keyRangeType = KeyRangeType.valueOf(keyType.trim());
          ByteBuffer start = parseKey(startKey, keyLen, byteOrder);
          ByteBuffer stop = parseKey(stopKey, keyLen, byteOrder);

          final KeyRange<ByteBuffer> keyRange = new KeyRange<>(keyRangeType, start, stop);
          try (Txn<ByteBuffer> txn = env.txnRead();
               Stream<KeyVal<ByteBuffer>> stream = dbi.stream(txn, keyRange)) {
            stream.forEach(kv -> {
              try {
                final long key = getLong(kv.key(), byteOrder);
                final long val = getLong(kv.val(), byteOrder);
                writer.append("[");
                writer.append(String.valueOf(key));
                writer.append(" ");
                writer.append(String.valueOf(val));
                writer.append("]");
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            });
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
      if (ByteOrder.LITTLE_ENDIAN.equals(byteOrder)) {
        if (keyLen == Integer.BYTES) {
          return bbLeInt(Integer.parseInt(key.trim()));
        } else {
          return bbLeLong(Long.parseLong(key.trim()));
        }
      } else {
        if (keyLen == Integer.BYTES) {
          return bb(Integer.parseInt(key.trim()));
        } else {
          return bb(Long.parseLong(key.trim()));
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

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createIntegerDBPopulator() {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        c.put(bbLeInt(Integer.MIN_VALUE), bb(1));
        c.put(bbLeInt(-1000), bb(2));
        c.put(bbLeInt(0), bb(3));
        c.put(bbLeInt(1000), bb(4));
        c.put(bbLeInt(Integer.MAX_VALUE), bb(5));
        txn.commit();
      }
    };
  }

  private BiConsumer<Env<ByteBuffer>, Dbi<ByteBuffer>> createLongDBPopulator() {
    return (env, dbi) -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = dbi.openCursor(txn);
        c.put(bbLeLong(Long.MIN_VALUE), bb(1));
        c.put(bbLeLong(-1000), bb(2));
        c.put(bbLeLong(0), bb(3));
        c.put(bbLeLong(1000), bb(4));
        c.put(bbLeLong(Long.MAX_VALUE), bb(5));
        txn.commit();
      }
    };
  }

  private void populateDatabase(
          final Env<ByteBuffer> env, final Dbi<ByteBuffer> dbi, final int copies) {
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
  }

  private String readFile(final File file) throws IOException {
    final StringBuilder result = new StringBuilder();
    try (final Reader reader = new FileReader(file)) {
      final char[] cbuf = new char[4096];
      int nread;
      while ((nread = reader.read(cbuf, 0, cbuf.length)) != -1) {
        result.append(cbuf, 0, nread);
      }
    }
    return result.toString();
  }

  static ByteBuffer bbLeInt(final int value) {
    final ByteBuffer bb = allocateDirect(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(value).flip();
    return bb;
  }

  static ByteBuffer bbLeLong(final long value) {
    final ByteBuffer bb = allocateDirect(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(value).flip();
    return bb;
  }
}
