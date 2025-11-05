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
import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.TestUtils.getString;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DbiBuilderTest {

  private Path file;
  private Env<ByteBuffer> env;

  @BeforeEach
  public void before() {
    file = FileUtil.createTempFile();
    env = create()
        .setMapSize(MEBIBYTES.toBytes(64))
        .setMaxReaders(2)
        .setMaxDbs(2)
        .setEnvFlags(MDB_NOSUBDIR)
        .open(file);
  }

  @AfterEach
  public void after() {
    env.close();
    FileUtil.delete(file);
  }

  @Test
  public void unnamed() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .withoutDbName()
        .withDefaultComparator()
        .setDbiFlags(DbiFlags.MDB_CREATE)
        .open();
    assertThat(dbi.getName()).isNull();
    assertThat(dbi.getNameAsString()).isEmpty();
    assertThat(env.getDbiNames()).isEmpty();
    assertPutAndGet(dbi);
  }

  @Test
  public void named() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .setDbName("foo")
        .withDefaultComparator()
        .setDbiFlags(DbiFlags.MDB_CREATE)
        .open();

    assertPutAndGet(dbi);

    assertThat(env.getDbiNames()).hasSize(1);
    assertThat(new String(env.getDbiNames().get(0), StandardCharsets.UTF_8))
        .isEqualTo("foo");
    assertThat(dbi.getNameAsString())
        .isEqualTo("foo");
    assertThat(dbi.getNameAsString(StandardCharsets.UTF_8))
        .isEqualTo("foo");
  }

  @Test
  public void named2() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .setDbName("foo".getBytes(StandardCharsets.US_ASCII))
        .withDefaultComparator()
        .setDbiFlags(DbiFlags.MDB_CREATE)
        .open();

    assertPutAndGet(dbi);

    assertThat(env.getDbiNames()).hasSize(1);
    assertThat(new String(env.getDbiNames().get(0), StandardCharsets.US_ASCII))
        .isEqualTo("foo");
    assertThat(dbi.getNameAsString())
        .isEqualTo("foo");
    assertThat(dbi.getNameAsString(StandardCharsets.US_ASCII))
        .isEqualTo("foo");
  }

  @Test
  public void nativeComparator() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .setDbName("foo")
        .withNativeComparator()
        .addDbiFlags(DbiFlags.MDB_CREATE)
        .open();

    assertPutAndGet(dbi);
    assertThat(env.getDbiNames()).hasSize(1);
  }

  @Test
  public void callback() {
    final Comparator<ByteBuffer> proxyOptimal = ByteBufferProxy.PROXY_OPTIMAL.getComparator();
    // Compare on key length, falling back to default
    final Comparator<ByteBuffer> comparator = (o1, o2) -> {
      final int res = Integer.compare(o1.remaining(), o2.remaining());
      if (res == 0) {
        return proxyOptimal.compare(o1, o2);
      } else {
        return res;
      }
    };

    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .setDbName("foo")
        .withCallbackComparator(ignored -> comparator)
        .addDbiFlags(DbiFlags.MDB_CREATE)
        .open();

    TestUtils.doWithWriteTxn(env, txn -> {
      dbi.put(txn, bb("fox"), bb("val_1"));
      dbi.put(txn, bb("rabbit"), bb("val_2"));
      dbi.put(txn, bb("deer"), bb("val_3"));
      dbi.put(txn, bb("badger"), bb("val_4"));
      txn.commit();
    });

    final List<String> keys = new ArrayList<>();
    TestUtils.doWithReadTxn(env, txn -> {
      try (CursorIterable<ByteBuffer> cursorIterable = dbi.iterate(txn)) {
        final Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = cursorIterable.iterator();
        iterator.forEachRemaining(keyVal -> {
          keys.add(getString(keyVal.key()));
        });
      }
    });
    assertThat(keys).containsExactly(
        "fox",
        "deer",
        "badger",
        "rabbit");
  }

  @Test
  public void flags() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .setDbName("foo")
        .withDefaultComparator()
        .setDbiFlags(DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED) // Will get overwritten
        .setDbiFlags() // clear them
        .addDbiFlags(DbiFlags.MDB_CREATE) // Not a dbi flag as far as lmdb is concerned.
        .addDbiFlags(DbiFlags.MDB_INTEGERKEY)
        .addDbiFlags(DbiFlags.MDB_REVERSEKEY)
        .open();

    assertPutAndGet(dbi);

    assertThat(env.getDbiNames()).hasSize(1);
    assertThat(new String(env.getDbiNames().get(0), StandardCharsets.UTF_8))
        .isEqualTo("foo");

    TestUtils.doWithReadTxn(env, readTxn -> {
      assertThat(dbi.listFlags(readTxn))
          .containsExactlyInAnyOrder(
              DbiFlags.MDB_INTEGERKEY,
              DbiFlags.MDB_REVERSEKEY);
    });
  }

  private void assertPutAndGet(Dbi<ByteBuffer> dbi) {
    try (Txn<ByteBuffer> writeTxn = env.txnWrite()) {
      dbi.put(writeTxn, bb(123), bb(123_000));
      writeTxn.commit();
    }

    try (Txn<ByteBuffer> readTxn = env.txnRead()) {
      final ByteBuffer byteBuffer = dbi.get(readTxn, bb(123));
      assertThat(byteBuffer).isNotNull();
      final int val = byteBuffer.getInt();
      assertThat(val).isEqualTo(123_000);
    }
  }
}
