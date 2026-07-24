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

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Regression test for reverse prefix iteration when the prefix ends in {@code 0xFF}.
 *
 * <p>Reverse prefix iteration seeks to the "prefix successor" (one greater than the prefix) and
 * steps back. If that successor over-shoots — e.g. prefix {@code {0x01,0xFF}} producing {@code
 * {0x02,0xFF}} instead of the tight {@code {0x02}} — an unrelated higher key that sorts between the
 * last prefix match and the over-shot successor is landed on, the prefix check fails, and iteration
 * wrongly yields nothing.
 */
final class LmdbPrefixReversedSuccessorTest {

  private static byte[] key(final int... values) {
    final byte[] array = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      array[i] = (byte) values[i];
    }
    return array;
  }

  @Test
  void reversePrefixEndingInFfReturnsAllMatches() throws IOException {
    final byte[] prefix = key(0x01, 0xFF);
    // Expected matches, in reverse (descending) order.
    final byte[] match2 = key(0x01, 0xFF, 0x05);
    final byte[] match1 = key(0x01, 0xFF);

    final Path path = Files.createTempDirectory("lmdb");
    try (Env<byte[]> env = Env.create(PROXY_BA).setMapSize(1, ByteUnit.MEBIBYTES).open(path)) {
      final Dbi<byte[]> dbi =
          env.openDbi("test".getBytes(StandardCharsets.UTF_8), DbiFlags.MDB_CREATE);
      try (Txn<byte[]> txn = env.txnWrite()) {
        final byte[] empty = new byte[0];
        dbi.put(txn, match1, empty);
        dbi.put(txn, match2, empty);
        // An unrelated key that sorts just above the prefix range and below an over-shot successor.
        dbi.put(txn, key(0x02, 0x00), empty);
        txn.commit();
      }

      try (Txn<byte[]> txn = env.txnRead()) {
        // LmdbIterable path.
        final List<byte[]> viaIterable = new ArrayList<>();
        try (LmdbIterable<byte[]> it = dbi.newIterate(txn, KeyRange.prefixBackward(prefix))) {
          for (final CursorIterable.KeyVal<byte[]> kv : it) {
            viaIterable.add(kv.key().clone());
          }
        }
        assertThat(viaIterable).containsExactly(match2, match1);

        // LmdbStream path (uses the same prefix-successor logic).
        final List<byte[]> viaStream;
        try (Stream<CursorIterable.KeyVal<byte[]>> s =
            dbi.stream(txn, KeyRange.prefixBackward(prefix))) {
          viaStream = s.map(kv -> kv.key().clone()).collect(toList());
        }
        assertThat(viaStream).containsExactly(match2, match1);
      }
    } catch (final UncheckedIOException e) {
      throw e;
    } finally {
      FileUtil.deleteDir(path);
    }
  }
}
