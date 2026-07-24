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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@link Spliterator} contract reported by {@link LmdbStream}.
 *
 * <p>The stream produces entries in the database's key order but does not expose a {@link
 * java.util.Comparator} over {@link CursorIterable.KeyVal}. It must therefore report {@code
 * ORDERED} but not {@code SORTED}, and {@link Spliterator#getComparator()} must throw as required
 * by the contract for a non-sorted spliterator.
 */
final class LmdbStreamCharacteristicsTest {

  @Test
  void reportsOrderedNonNullButNotSortedOrDistinct() {
    withStreamSpliterator(
        spliterator -> {
          assertThat(spliterator.hasCharacteristics(Spliterator.ORDERED)).isTrue();
          assertThat(spliterator.hasCharacteristics(Spliterator.NONNULL)).isTrue();
          assertThat(spliterator.hasCharacteristics(Spliterator.SORTED)).isFalse();
          assertThat(spliterator.hasCharacteristics(Spliterator.DISTINCT)).isFalse();
        });
  }

  @Test
  void getComparatorThrowsBecauseNotSorted() {
    withStreamSpliterator(
        spliterator ->
            assertThatThrownBy(spliterator::getComparator)
                .isInstanceOf(IllegalStateException.class));
  }

  private void withStreamSpliterator(
      final Consumer<Spliterator<CursorIterable.KeyVal<ByteBuffer>>> assertion) {
    try {
      final Path path = Files.createTempDirectory("lmdb");
      try (final Env<ByteBuffer> env = Env.create().setMapSize(1, ByteUnit.MEBIBYTES).open(path)) {
        final Dbi<ByteBuffer> dbi =
            env.openDbi("test".getBytes(StandardCharsets.UTF_8), DbiFlags.MDB_CREATE);
        try (final Txn<ByteBuffer> txn = env.txnWrite()) {
          final ByteBuffer value = ByteBuffer.allocateDirect(0);
          for (int i = 0; i < 5; i++) {
            final ByteBuffer key = ByteBuffer.allocateDirect(Integer.BYTES);
            key.putInt(i).flip();
            dbi.put(txn, key, value);
          }
          txn.commit();
        }
        try (final Txn<ByteBuffer> txn = env.txnRead()) {
          try (final Stream<CursorIterable.KeyVal<ByteBuffer>> stream =
              dbi.stream(txn, KeyRange.all())) {
            assertion.accept(stream.spliterator());
          }
        }
      }
      FileUtil.deleteDir(path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
