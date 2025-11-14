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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ByteBufProxyTest {

  @Test
  public void verifyComparators_int() {
    final Random random = new Random(203948);
    final ByteBufProxy byteBufProxy = new ByteBufProxy(PooledByteBufAllocator.DEFAULT);
    final ByteBuf buffer1native = byteBufProxy.allocate().capacity(Integer.BYTES);
    final ByteBuf buffer2native = byteBufProxy.allocate().capacity(Integer.BYTES);
    final ByteBuf buffer1be = byteBufProxy.allocate().capacity(Integer.BYTES);
    final ByteBuf buffer2be = byteBufProxy.allocate().capacity(Integer.BYTES);
    final int[] values = random.ints().filter(i -> i >= 0).limit(5_000_000).toArray();

    final LinkedHashMap<CompareType, Comparator<ByteBuf>> comparators = new LinkedHashMap<>();
    comparators.put(CompareType.INTEGER_KEY, ByteBufProxy::compareAsIntegerKeys);
    comparators.put(CompareType.LEXICOGRAPHIC, ByteBufProxy::compareLexicographically);

    final LinkedHashMap<CompareType, ComparatorResult> results =
        new LinkedHashMap<>(comparators.size());
    final Set<ComparatorResult> uniqueResults = new HashSet<>(comparators.size());

    for (int i = 1; i < values.length; i++) {
      resetBuffer(buffer1native);
      resetBuffer(buffer2native);
      resetBuffer(buffer1be);
      resetBuffer(buffer2be);

      final int val1 = values[i - 1];
      final int val2 = values[i];
      if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
        buffer1native.writeIntLE(val1);
        buffer2native.writeIntLE(val2);
      } else {
        buffer1native.writeInt(val1);
        buffer2native.writeInt(val2);
      }
      buffer1be.writeInt(val1);
      buffer2be.writeInt(val2);

      Assertions.assertThat(buffer1native.readableBytes()).isEqualTo(Integer.BYTES);
      Assertions.assertThat(buffer2native.readableBytes()).isEqualTo(Integer.BYTES);
      Assertions.assertThat(buffer1be.readableBytes()).isEqualTo(Integer.BYTES);
      Assertions.assertThat(buffer2be.readableBytes()).isEqualTo(Integer.BYTES);

      uniqueResults.clear();

      // Make sure all comparators give the same result for the same inputs
      comparators.forEach(
          (compareType, comparator) -> {
            final ComparatorResult result;
            // IntegerKey comparator expects keys to have been written in native order so need
            // different buffers.
            if (compareType == CompareType.INTEGER_KEY) {
              result = TestUtils.compare(comparator, buffer1native, buffer2native);
            } else {
              result = TestUtils.compare(comparator, buffer1be, buffer2be);
            }
            results.put(compareType, result);
            uniqueResults.add(result);
          });

      if (uniqueResults.size() != 1) {
        Assertions.fail(
            "Comparator mismatch for values: " + val1 + " and " + val2 + ". Results: " + results);
      }
    }
  }

  @Test
  public void verifyComparators_long() {
    final Random random = new Random(203948);
    final ByteBufProxy byteBufProxy = new ByteBufProxy(PooledByteBufAllocator.DEFAULT);
    final ByteBuf buffer1native = byteBufProxy.allocate().capacity(Long.BYTES);
    final ByteBuf buffer2native = byteBufProxy.allocate().capacity(Long.BYTES);
    final ByteBuf buffer1be = byteBufProxy.allocate().capacity(Long.BYTES);
    final ByteBuf buffer2be = byteBufProxy.allocate().capacity(Long.BYTES);
    final long[] values = random.longs().filter(i -> i >= 0).limit(5_000_000).toArray();

    final LinkedHashMap<CompareType, Comparator<ByteBuf>> comparators = new LinkedHashMap<>();
    comparators.put(CompareType.INTEGER_KEY, ByteBufProxy::compareAsIntegerKeys);
    comparators.put(CompareType.LEXICOGRAPHIC, ByteBufProxy::compareLexicographically);

    final LinkedHashMap<CompareType, ComparatorResult> results =
        new LinkedHashMap<>(comparators.size());
    final Set<ComparatorResult> uniqueResults = new HashSet<>(comparators.size());

    for (int i = 1; i < values.length; i++) {
      resetBuffer(buffer1native);
      resetBuffer(buffer2native);
      resetBuffer(buffer1be);
      resetBuffer(buffer2be);

      final long val1 = values[i - 1];
      final long val2 = values[i];
      if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
        buffer1native.writeLongLE(val1);
        buffer2native.writeLongLE(val2);
      } else {
        buffer1native.writeLong(val1);
        buffer2native.writeLong(val2);
      }
      buffer1be.writeLong(val1);
      buffer2be.writeLong(val2);

      Assertions.assertThat(buffer1native.readableBytes()).isEqualTo(Long.BYTES);
      Assertions.assertThat(buffer2native.readableBytes()).isEqualTo(Long.BYTES);
      Assertions.assertThat(buffer1be.readableBytes()).isEqualTo(Long.BYTES);
      Assertions.assertThat(buffer2be.readableBytes()).isEqualTo(Long.BYTES);

      uniqueResults.clear();

      // Make sure all comparators give the same result for the same inputs
      comparators.forEach(
          (compareType, comparator) -> {
            final ComparatorResult result;
            // IntegerKey comparator expects keys to have been written in native order so need
            // different buffers.
            if (compareType == CompareType.INTEGER_KEY) {
              result = TestUtils.compare(comparator, buffer1native, buffer2native);
            } else {
              result = TestUtils.compare(comparator, buffer1be, buffer2be);
            }
            results.put(compareType, result);
            uniqueResults.add(result);
          });

      if (uniqueResults.size() != 1) {
        Assertions.fail(
            "Comparator mismatch for values: " + val1 + " and " + val2 + ". Results: " + results);
      }
    }
  }

  private static void resetBuffer(ByteBuf buffer1native) {
    buffer1native.resetReaderIndex();
    buffer1native.resetWriterIndex();
  }
}
