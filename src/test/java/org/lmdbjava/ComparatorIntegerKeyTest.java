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

import static io.netty.buffer.PooledByteBufAllocator.DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.ByteBufProxy.PROXY_NETTY;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ComparatorTest.ComparatorResult.EQUAL_TO;
import static org.lmdbjava.ComparatorTest.ComparatorResult.GREATER_THAN;
import static org.lmdbjava.ComparatorTest.ComparatorResult.LESS_THAN;
import static org.lmdbjava.ComparatorTest.ComparatorResult.get;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests comparator functions are consistent across buffers. */
public final class ComparatorIntegerKeyTest {

  static Stream<Arguments> comparatorProvider() {
    return Stream.of(
        Arguments.argumentSet("LongRunner", new DirectBufferRunner()),
        Arguments.argumentSet("DirectBufferRunner", new DirectBufferRunner()),
        Arguments.argumentSet("ByteBufferRunner", new ByteBufferRunner()),
        Arguments.argumentSet("NettyRunner", new NettyRunner()));
  }

  private static byte[] buffer(final int... bytes) {
    final byte[] array = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      array[i] = (byte) bytes[i];
    }
    return array;
  }

  @ParameterizedTest
  @MethodSource("comparatorProvider")
  void testLong(final ComparatorRunner comparator) {

    assertThat(get(comparator.compare(0L, 0L))).isEqualTo(EQUAL_TO);
    assertThat(get(comparator.compare(Long.MAX_VALUE, Long.MAX_VALUE))).isEqualTo(EQUAL_TO);

    assertThat(get(comparator.compare(0L, 1L))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(0L, Long.MAX_VALUE))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(0L, 10L))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(10L, 100L))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(10L, 100L))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(10L, 1000L))).isEqualTo(LESS_THAN);

    assertThat(get(comparator.compare(1L, 0L))).isEqualTo(GREATER_THAN);
    assertThat(get(comparator.compare(Long.MAX_VALUE, 0L))).isEqualTo(GREATER_THAN);
  }

  @ParameterizedTest
  @MethodSource("comparatorProvider")
  void testInt(final ComparatorRunner comparator) {

    assertThat(get(comparator.compare(0, 0))).isEqualTo(EQUAL_TO);
    assertThat(get(comparator.compare(Integer.MAX_VALUE, Integer.MAX_VALUE))).isEqualTo(EQUAL_TO);

    assertThat(get(comparator.compare(0, 1))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(0, Integer.MAX_VALUE))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(0, 10))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(10, 100))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(10, 100))).isEqualTo(LESS_THAN);
    assertThat(get(comparator.compare(10, 1000))).isEqualTo(LESS_THAN);

    assertThat(get(comparator.compare(1, 0))).isEqualTo(GREATER_THAN);
    assertThat(get(comparator.compare(Integer.MAX_VALUE, 0))).isEqualTo(GREATER_THAN);
  }

  @ParameterizedTest
  @MethodSource("comparatorProvider")
  void testRandomLong(final ComparatorRunner runner) {
    final Random random = new Random(3239480);

    // 5mil random longs to compare
    final long[] values = random.longs().filter(i -> i >= 0).limit(5_000_000).toArray();

    for (int i = 1; i < values.length; i++) {
      final long long1 = values[i - 1];
      final long long2 = values[i];
      // Make sure the comparator under test gives the same outcome as just comparing two longs
      final ComparatorTest.ComparatorResult result = get(runner.compare(long1, long2));
      final ComparatorTest.ComparatorResult expectedResult = get(Long.compare(long1, long2));

      assertThat(result)
          .withFailMessage(
              () ->
                  "Compare mismatch - long1: "
                      + long1
                      + ", long2: "
                      + long2
                      + ", expected: "
                      + expectedResult
                      + ", actual: "
                      + result)
          .isEqualTo(expectedResult);

      final ComparatorTest.ComparatorResult result2 = get(runner.compare(long2, long1));
      final ComparatorTest.ComparatorResult expectedResult2 = expectedResult.opposite();

      assertThat(result)
          .withFailMessage(
              () ->
                  "Compare mismatch for - long2: "
                      + long2
                      + ", long1: "
                      + long1
                      + ", expected2: "
                      + expectedResult2
                      + ", actual2: "
                      + result2)
          .isEqualTo(expectedResult);
    }
  }

  @ParameterizedTest
  @MethodSource("comparatorProvider")
  void testRandomInt(final ComparatorRunner runner) {
    final Random random = new Random(3239480);

    // 5mil random ints to compare
    final int[] values = random.ints().filter(i -> i >= 0).limit(5_000_000).toArray();

    for (int i = 1; i < values.length; i++) {
      final int int1 = values[i - 1];
      final int int2 = values[i];
      // Make sure the comparator under test gives the same outcome as just comparing two ints
      final ComparatorTest.ComparatorResult result = get(runner.compare(int1, int2));
      final ComparatorTest.ComparatorResult expectedResult = get(Integer.compare(int1, int2));

      assertThat(result)
          .withFailMessage(
              () ->
                  "Compare mismatch for - int1: "
                      + int1
                      + ", int2: "
                      + int2
                      + ", expected: "
                      + expectedResult
                      + ", actual: "
                      + result)
          .isEqualTo(expectedResult);

      final ComparatorTest.ComparatorResult result2 = get(runner.compare(int2, int1));
      final ComparatorTest.ComparatorResult expectedResult2 = expectedResult.opposite();

      assertThat(result)
          .withFailMessage(
              () ->
                  "Compare mismatch for - int2: "
                      + int2
                      + ", int1: "
                      + int1
                      + ", expected2: "
                      + expectedResult2
                      + ", actual2: "
                      + result2)
          .isEqualTo(expectedResult);
    }
  }

  
  /** Tests {@link ByteBufferProxy}. */
  private static final class ByteBufferRunner implements ComparatorRunner {

    private static final Comparator<ByteBuffer> COMPARATOR =
        PROXY_OPTIMAL.getComparator(DbiFlags.MDB_INTEGERKEY);

    @Override
    public int compare(long long1, long long2) {
      // Convert arrays to buffers that are larger than the array, with
      // limit set at the array length. One buffer bigger than the other.
      ByteBuffer o1b = longToBuffer(long1, Long.BYTES * 3);
      ByteBuffer o2b = longToBuffer(long2, Long.BYTES * 2);
      final int result = COMPARATOR.compare(o1b, o2b);

      // Now swap which buffer is bigger
      o1b = longToBuffer(long1, Long.BYTES * 2);
      o2b = longToBuffer(long2, Long.BYTES * 3);
      final int result2 = COMPARATOR.compare(o1b, o2b);

      assertThat(result2).isEqualTo(result);

      // Now try with buffers sized to the array.
      o1b = longToBuffer(long1, Long.BYTES);
      o2b = longToBuffer(long2, Long.BYTES);
      final int result3 = COMPARATOR.compare(o1b, o2b);

      assertThat(result3).isEqualTo(result);
      return result;
    }

    @Override
    public int compare(int int1, int int2) {
      // Convert arrays to buffers that are larger than the array, with
      // limit set at the array length. One buffer bigger than the other.
      ByteBuffer o1b = intToBuffer(int1, Integer.BYTES * 3);
      ByteBuffer o2b = intToBuffer(int2, Integer.BYTES * 2);
      final int result = COMPARATOR.compare(o1b, o2b);

      // Now swap which buffer is bigger
      o1b = intToBuffer(int1, Integer.BYTES * 2);
      o2b = intToBuffer(int2, Integer.BYTES * 3);
      final int result2 = COMPARATOR.compare(o1b, o2b);

      assertThat(result2).isEqualTo(result);

      // Now try with buffers sized to the array.
      o1b = intToBuffer(int1, Integer.BYTES);
      o2b = intToBuffer(int2, Integer.BYTES);
      final int result3 = COMPARATOR.compare(o1b, o2b);

      assertThat(result3).isEqualTo(result);
      return result;
    }

    private ByteBuffer longToBuffer(final long val, final int bufferCapacity) {
      final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferCapacity);
      byteBuffer.order(ByteOrder.nativeOrder());
      byteBuffer.putLong(0, val);
      byteBuffer.limit(Long.BYTES);
      byteBuffer.position(0);
      return byteBuffer;
    }

    private ByteBuffer intToBuffer(final int val, final int bufferCapacity) {
      final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferCapacity);
      byteBuffer.order(ByteOrder.nativeOrder());
      byteBuffer.putInt(0, val);
      byteBuffer.limit(Integer.BYTES);
      byteBuffer.position(0);
      return byteBuffer;
    }
  }

  
  /** Tests {@link DirectBufferProxy}. */
  private static final class DirectBufferRunner implements ComparatorRunner {
    private static final Comparator<DirectBuffer> COMPARATOR =
        PROXY_DB.getComparator(DbiFlags.MDB_INTEGERKEY);

    @Override
    public int compare(long long1, long long2) {
      final UnsafeBuffer o1b = new UnsafeBuffer(new byte[Long.BYTES]);
      final UnsafeBuffer o2b = new UnsafeBuffer(new byte[Long.BYTES]);
      o1b.putLong(0, long1, ByteOrder.nativeOrder());
      o2b.putLong(0, long2, ByteOrder.nativeOrder());
      return COMPARATOR.compare(o1b, o2b);
    }

    @Override
    public int compare(int int1, int int2) {
      final UnsafeBuffer o1b = new UnsafeBuffer(new byte[Integer.BYTES]);
      final UnsafeBuffer o2b = new UnsafeBuffer(new byte[Integer.BYTES]);
      o1b.putInt(0, int1, ByteOrder.nativeOrder());
      o2b.putInt(0, int2, ByteOrder.nativeOrder());
      return COMPARATOR.compare(o1b, o2b);
    }
  }

  /** Tests {@link ByteBufProxy}. */
  private static final class NettyRunner implements ComparatorRunner {

    private static final Comparator<ByteBuf> COMPARATOR =
        PROXY_NETTY.getComparator(DbiFlags.MDB_INTEGERKEY);

    @Override
    public int compare(long long1, long long2) {
      final ByteBuf o1b = DEFAULT.directBuffer(Long.BYTES);
      final ByteBuf o2b = DEFAULT.directBuffer(Long.BYTES);
      if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
        o1b.writeLongLE(long1);
        o2b.writeLongLE(long2);
      } else {
        o1b.writeLong(long1);
        o2b.writeLong(long2);
      }
      o1b.resetReaderIndex();
      o2b.resetReaderIndex();
      final int res = COMPARATOR.compare(o1b, o2b);
      o1b.release();
      o2b.release();
      return res;
    }

    @Override
    public int compare(int int1, int int2) {
      final ByteBuf o1b = DEFAULT.directBuffer(Integer.BYTES);
      final ByteBuf o2b = DEFAULT.directBuffer(Integer.BYTES);
      if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
        o1b.writeIntLE(int1);
        o2b.writeIntLE(int2);
      } else {
        o1b.writeInt(int1);
        o2b.writeInt(int2);
      }
      o1b.resetReaderIndex();
      o2b.resetReaderIndex();
      final int res = COMPARATOR.compare(o1b, o2b);
      o1b.release();
      o2b.release();
      return res;
    }
  }

  
  /** Interface that can test a {@link BufferProxy} <code>compare</code> method. */
  private interface ComparatorRunner {

    /**
     * Write the two longs to a buffer using native order and compare the resulting buffers.
     *
     * @param long1 lhs value
     * @param long2 rhs value
     * @return as per {@link Comparable}
     */
    int compare(final long long1, final long long2);

    /**
     * Write the two int to a buffer using native order and compare the resulting buffers.
     *
     * @param int1 lhs value
     * @param int2 rhs value
     * @return as per {@link Comparable}
     */
    int compare(final int int1, final int int2);
  }
}
