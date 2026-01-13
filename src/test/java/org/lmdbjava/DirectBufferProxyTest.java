/*
 * Copyright © 2016-2026 The LmdbJava Open Source Project
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

import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DirectBufferProxyTest {

  @Test
  public void verifyComparators_int() {
    final Random random = new Random(203948);
    final MutableDirectBuffer buffer1native = new UnsafeBuffer(new byte[Integer.BYTES]);
    final MutableDirectBuffer buffer2native = new UnsafeBuffer(new byte[Integer.BYTES]);
    final MutableDirectBuffer buffer1be = new UnsafeBuffer(new byte[Integer.BYTES]);
    final MutableDirectBuffer buffer2be = new UnsafeBuffer(new byte[Integer.BYTES]);
    final int[] values = random.ints().filter(i -> i >= 0).limit(5_000_000).toArray();

    final LinkedHashMap<CompareType, Comparator<DirectBuffer>> comparators = new LinkedHashMap<>();
    comparators.put(CompareType.INTEGER_KEY, DirectBufferProxy::compareAsIntegerKeys);
    comparators.put(CompareType.LEXICOGRAPHIC, DirectBufferProxy::compareLexicographically);

    final LinkedHashMap<CompareType, ComparatorResult> results =
        new LinkedHashMap<>(comparators.size());
    final Set<ComparatorResult> uniqueResults = new HashSet<>(comparators.size());

    for (int i = 1; i < values.length; i++) {
      final int val1 = values[i - 1];
      final int val2 = values[i];
      buffer1native.putInt(0, val1, ByteOrder.nativeOrder());
      buffer2native.putInt(0, val2, ByteOrder.nativeOrder());
      buffer1be.putInt(0, val1, ByteOrder.BIG_ENDIAN);
      buffer2be.putInt(0, val2, ByteOrder.BIG_ENDIAN);

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
    final MutableDirectBuffer buffer1native = new UnsafeBuffer(new byte[Long.BYTES]);
    final MutableDirectBuffer buffer2native = new UnsafeBuffer(new byte[Long.BYTES]);
    final MutableDirectBuffer buffer1be = new UnsafeBuffer(new byte[Long.BYTES]);
    final MutableDirectBuffer buffer2be = new UnsafeBuffer(new byte[Long.BYTES]);
    final long[] values = random.longs().filter(i -> i >= 0).limit(5_000_000).toArray();

    final LinkedHashMap<CompareType, Comparator<DirectBuffer>> comparators = new LinkedHashMap<>();
    comparators.put(CompareType.INTEGER_KEY, DirectBufferProxy::compareAsIntegerKeys);
    comparators.put(CompareType.LEXICOGRAPHIC, DirectBufferProxy::compareLexicographically);

    final LinkedHashMap<CompareType, ComparatorResult> results =
        new LinkedHashMap<>(comparators.size());
    final Set<ComparatorResult> uniqueResults = new HashSet<>(comparators.size());

    for (int i = 1; i < values.length; i++) {
      final long val1 = values[i - 1];
      final long val2 = values[i];
      buffer1native.putLong(0, val1, ByteOrder.nativeOrder());
      buffer2native.putLong(0, val2, ByteOrder.nativeOrder());
      buffer1be.putLong(0, val1, ByteOrder.BIG_ENDIAN);
      buffer2be.putLong(0, val2, ByteOrder.BIG_ENDIAN);

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
}
