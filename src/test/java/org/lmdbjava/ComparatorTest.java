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
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufProxy.PROXY_NETTY;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ComparatorResult.EQUAL_TO;
import static org.lmdbjava.ComparatorResult.GREATER_THAN;
import static org.lmdbjava.ComparatorResult.LESS_THAN;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;

import com.google.common.primitives.SignedBytes;
import com.google.common.primitives.UnsignedBytes;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

/** Tests comparator functions are consistent across buffers. */
public final class ComparatorTest {

  // H = 1 (high), L = 0 (low), X = byte not set in buffer
  private static final byte[] HL = buffer(1, 0);
  private static final byte[] HLLLLLLL = buffer(1, 0, 0, 0, 0, 0, 0, 0);
  private static final byte[] HLLLLLLX = buffer(1, 0, 0, 0, 0, 0, 0);
  private static final byte[] HX = buffer(1);
  private static final byte[] LH = buffer(0, 1);
  private static final byte[] LHLLLLLL = buffer(0, 1, 0, 0, 0, 0, 0, 0);
  private static final byte[] LL = buffer(0, 0);
  private static final byte[] LLLLLLLL = buffer(0, 0, 0, 0, 0, 0, 0, 0);
  private static final byte[] LLLLLLLX = buffer(0, 0, 0, 0, 0, 0, 0);
  private static final byte[] LX = buffer(0);
  private static final byte[] XX = buffer();

  static class MyArgumentProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations parameters, ExtensionContext context) {
      return Stream.of(
          Arguments.argumentSet("StringRunner", new StringRunner()),
          Arguments.argumentSet("DirectBufferRunner", new DirectBufferRunner()),
          Arguments.argumentSet("ByteArrayRunner", new ByteArrayRunner()),
          Arguments.argumentSet("UnsignedByteArrayRunner", new UnsignedByteArrayRunner()),
          Arguments.argumentSet("ByteBufferRunner", new ByteBufferRunner()),
          Arguments.argumentSet("NettyRunner", new NettyRunner()),
          Arguments.argumentSet("GuavaUnsignedBytes", new GuavaUnsignedBytes()),
          Arguments.argumentSet("GuavaSignedBytes", new GuavaSignedBytes()));
    }
  }

  private static byte[] buffer(final int... bytes) {
    final byte[] array = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      array[i] = (byte) bytes[i];
    }
    return array;
  }

  @ParameterizedTest
  @ArgumentsSource(MyArgumentProvider.class)
  void atLeastOneBufferHasEightBytes(final ComparatorRunner comparator) {
    assertThat(TestUtils.compare(comparator, HLLLLLLL, LLLLLLLL)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LLLLLLLL, HLLLLLLL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, LHLLLLLL, LLLLLLLL)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LLLLLLLL, LHLLLLLL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, LLLLLLLL, LLLLLLLX)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LLLLLLLX, LLLLLLLL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, HLLLLLLL, HLLLLLLX)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, HLLLLLLX, HLLLLLLL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, HLLLLLLX, LHLLLLLL)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LHLLLLLL, HLLLLLLX)).isEqualTo(LESS_THAN);
  }

  @ParameterizedTest
  @ArgumentsSource(MyArgumentProvider.class)
  void buffersOfTwoBytes(final ComparatorRunner comparator) {
    assertThat(TestUtils.compare(comparator, LL, XX)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, XX, LL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, LL, LX)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LX, LL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, LH, LX)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LX, HL)).isEqualTo(LESS_THAN);

    assertThat(TestUtils.compare(comparator, HX, LL)).isEqualTo(GREATER_THAN);
    assertThat(TestUtils.compare(comparator, LH, HX)).isEqualTo(LESS_THAN);
  }

  @ParameterizedTest
  @ArgumentsSource(MyArgumentProvider.class)
  void equalBuffers(final ComparatorRunner comparator) {
    assertThat(TestUtils.compare(comparator, LL, LL)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, HX, HX)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, LH, LH)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, LL, LL)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, LX, LX)).isEqualTo(EQUAL_TO);

    assertThat(TestUtils.compare(comparator, HLLLLLLL, HLLLLLLL)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, HLLLLLLX, HLLLLLLX)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, LHLLLLLL, LHLLLLLL)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, LLLLLLLL, LLLLLLLL)).isEqualTo(EQUAL_TO);
    assertThat(TestUtils.compare(comparator, LLLLLLLX, LLLLLLLX)).isEqualTo(EQUAL_TO);
  }

  /** Tests {@link ByteArrayProxy}. */
  private static final class ByteArrayRunner implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final Comparator<byte[]> c = PROXY_BA.getComparator();
      return c.compare(o1, o2);
    }
  }

  /** Tests {@link ByteArrayProxy} (unsigned). */
  private static final class UnsignedByteArrayRunner implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final Comparator<byte[]> c = PROXY_BA.getComparator();
      return c.compare(o1, o2);
    }
  }

  /** Tests {@link ByteBufferProxy}. */
  private static final class ByteBufferRunner implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final Comparator<ByteBuffer> c = PROXY_OPTIMAL.getComparator();

      // Convert arrays to buffers that are larger than the array, with
      // limit set at the array length. One buffer bigger than the other.
      ByteBuffer o1b = arrayToBuffer(o1, o1.length * 3);
      ByteBuffer o2b = arrayToBuffer(o2, o2.length * 2);
      final int result = c.compare(o1b, o2b);

      // Now swap which buffer is bigger
      o1b = arrayToBuffer(o1, o1.length * 2);
      o2b = arrayToBuffer(o2, o2.length * 3);
      final int result2 = c.compare(o1b, o2b);

      assertThat(result2).isEqualTo(result);

      // Now try with buffers sized to the array.
      o1b = ByteBuffer.wrap(o1);
      o2b = ByteBuffer.wrap(o2);
      final int result3 = c.compare(o1b, o2b);

      assertThat(result3).isEqualTo(result);

      return result;
    }

    private ByteBuffer arrayToBuffer(final byte[] arr, final int bufferCapacity) {
      if (bufferCapacity < arr.length) {
        throw new IllegalArgumentException("bufferCapacity < arr.length");
      }
      final byte[] newArr = Arrays.copyOf(arr, bufferCapacity);
      final ByteBuffer byteBuffer = ByteBuffer.wrap(newArr);
      byteBuffer.limit(arr.length);
      byteBuffer.position(0);
      return byteBuffer;
    }
  }

  /** Tests {@link DirectBufferProxy}. */
  private static final class DirectBufferRunner implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final DirectBuffer o1b = new UnsafeBuffer(o1);
      final DirectBuffer o2b = new UnsafeBuffer(o2);
      final Comparator<DirectBuffer> c = PROXY_DB.getComparator();
      return c.compare(o1b, o2b);
    }
  }

  /** Tests using Guava's {@link SignedBytes} comparator. */
  private static final class GuavaSignedBytes implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final Comparator<byte[]> c = SignedBytes.lexicographicalComparator();
      return c.compare(o1, o2);
    }
  }

  /** Tests using Guava's {@link UnsignedBytes} comparator. */
  private static final class GuavaUnsignedBytes implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final Comparator<byte[]> c = UnsignedBytes.lexicographicalComparator();
      return c.compare(o1, o2);
    }
  }

  /** Tests {@link ByteBufProxy}. */
  private static final class NettyRunner implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final ByteBuf o1b = DEFAULT.directBuffer(o1.length);
      final ByteBuf o2b = DEFAULT.directBuffer(o2.length);
      o1b.writeBytes(o1);
      o2b.writeBytes(o2);
      final Comparator<ByteBuf> c = PROXY_NETTY.getComparator();
      return c.compare(o1b, o2b);
    }
  }

  /**
   * Tests {@link String} by providing a reference implementation of what a comparator involving
   * ASCII-encoded bytes should return.
   */
  private static final class StringRunner implements ComparatorRunner {

    @Override
    public int compare(final byte[] o1, final byte[] o2) {
      final String o1s = new String(o1, US_ASCII);
      final String o2s = new String(o2, US_ASCII);
      return o1s.compareTo(o2s);
    }
  }

  /** Interface that can test a {@link BufferProxy} <code>compare</code> method. */
  private interface ComparatorRunner extends Comparator<byte[]> {

    /**
     * Convert the passed byte arrays into the proxy's relevant buffer type and then invoke the
     * comparator.
     *
     * @param o1 lhs buffer content
     * @param o2 rhs buffer content
     * @return as per {@link Comparable}
     */
    @Override
    int compare(byte[] o1, byte[] o2);
  }
}
