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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Coverage for the newer {@link Dbi} iterate/stream API surface added for gh-269 that lacked direct
 * tests: the {@link EntryConsumer} overloads of {@code newIterate}, the name accessors and {@code
 * toString}, and the single-use guarantee of {@link LmdbIterable}.
 */
final class DbiIterateApiTest {

  private TempDir tempDir;
  private Env<ByteBuffer> env;
  private Dbi<ByteBuffer> dbi;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
    final Path file = tempDir.createTempFile();
    env =
        create()
            .setMapSize(64, ByteUnit.MEBIBYTES)
            .setMaxReaders(2)
            .setMaxDbs(2)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);
    dbi = env.openDbi(DB_1, MDB_CREATE);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      for (int i = 1; i <= 5; i++) {
        dbi.put(txn, bb(i), bb(i));
      }
      txn.commit();
    }
  }

  @AfterEach
  void afterEach() {
    env.close();
    tempDir.cleanup();
  }

  @Test
  void newIterateWithEntryConsumerVisitsEveryEntry() {
    final List<Integer> keys = new ArrayList<>();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      dbi.newIterate(txn, (k, v) -> keys.add(k.getInt(0)));
    }
    assertThat(keys).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  void newIterateWithEntryConsumerHonoursKeyRange() {
    final List<Integer> keys = new ArrayList<>();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      dbi.newIterate(txn, KeyRange.closed(bb(2), bb(4)), (k, v) -> keys.add(k.getInt(0)));
    }
    assertThat(keys).containsExactly(2, 3, 4);
  }

  @Test
  void streamCollectsRange() {
    final List<Integer> keys;
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (Stream<CursorIterable.KeyVal<ByteBuffer>> stream =
          dbi.stream(txn, KeyRange.atLeast(bb(3)))) {
        keys = stream.map(kv -> kv.key().getInt(0)).collect(Collectors.toList());
      }
    }
    assertThat(keys).containsExactly(3, 4, 5);
  }

  @Test
  void lmdbIterableIsSingleUse() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (LmdbIterable<ByteBuffer> iterable = dbi.newIterate(txn)) {
        iterable.iterator();
        assertThatThrownBy(iterable::iterator).isInstanceOf(IllegalStateException.class);
      }
    }
  }

  @Test
  void getNameAsStringForNamedDb() {
    assertThat(dbi.getNameAsString()).isEqualTo(DB_1);
    assertThat(dbi.getNameAsString(UTF_8)).isEqualTo(DB_1);
  }

  @Test
  void getNameAsStringForUnnamedDbIsEmpty() {
    final Dbi<ByteBuffer> unnamed = env.openDbi((byte[]) null, DbiFlagSet.EMPTY);
    assertThat(unnamed.getNameAsString()).isEmpty();
  }

  @Test
  void toStringContainsName() {
    assertThat(dbi.toString()).contains(DB_1);
  }
}
