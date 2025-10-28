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
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufProxy.PROXY_NETTY;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ComparatorTest.ComparatorResult.EQUAL_TO;
import static org.lmdbjava.ComparatorTest.ComparatorResult.GREATER_THAN;
import static org.lmdbjava.ComparatorTest.ComparatorResult.LESS_THAN;
import static org.lmdbjava.ComparatorTest.ComparatorResult.get;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;

import com.google.common.primitives.SignedBytes;
import com.google.common.primitives.UnsignedBytes;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests comparator functions are consistent across buffers. */
@RunWith(Parameterized.class)
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

  /** Injected by {@link #data()} with appropriate runner. */
  @Parameter public ComparatorRunner comparator;

  @Parameters(name = "{index}: comparable: {0}")
  public static Object[] data() {
    final ComparatorRunner string = new StringRunner();
    final ComparatorRunner db = new DirectBufferRunner();
    final ComparatorRunner ba = new ByteArrayRunner();
    final ComparatorRunner baUnsigned = new UnsignedByteArrayRunner();
    final ComparatorRunner bb = new ByteBufferRunner();
    final ComparatorRunner netty = new NettyRunner();
    final ComparatorRunner gub = new GuavaUnsignedBytes();
    final ComparatorRunner gsb = new GuavaSignedBytes();
    return new Object[] {string, db, ba, baUnsigned, bb, netty, gub, gsb};
  }

  private static byte[] buffer(final int... bytes) {
    final byte[] array = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      array[i] = (byte) bytes[i];
    }
    return array;
  }

  @Test
  public void atLeastOneBufferHasEightBytes() {
    assertThat(get(comparator.compare(HLLLLLLL, LLLLLLLL)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LLLLLLLL, HLLLLLLL)), is(LESS_THAN));

    assertThat(get(comparator.compare(LHLLLLLL, LLLLLLLL)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LLLLLLLL, LHLLLLLL)), is(LESS_THAN));

    assertThat(get(comparator.compare(LLLLLLLL, LLLLLLLX)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LLLLLLLX, LLLLLLLL)), is(LESS_THAN));

    assertThat(get(comparator.compare(HLLLLLLL, HLLLLLLX)), is(GREATER_THAN));
    assertThat(get(comparator.compare(HLLLLLLX, HLLLLLLL)), is(LESS_THAN));

    assertThat(get(comparator.compare(HLLLLLLX, LHLLLLLL)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LHLLLLLL, HLLLLLLX)), is(LESS_THAN));
  }

  @Test
  public void buffersOfTwoBytes() {
    assertThat(get(comparator.compare(LL, XX)), is(GREATER_THAN));
    assertThat(get(comparator.compare(XX, LL)), is(LESS_THAN));

    assertThat(get(comparator.compare(LL, LX)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LX, LL)), is(LESS_THAN));

    assertThat(get(comparator.compare(LH, LX)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LX, HL)), is(LESS_THAN));

    assertThat(get(comparator.compare(HX, LL)), is(GREATER_THAN));
    assertThat(get(comparator.compare(LH, HX)), is(LESS_THAN));
  }

  @Test
  public void equalBuffers() {
    assertThat(get(comparator.compare(LL, LL)), is(EQUAL_TO));
    assertThat(get(comparator.compare(HX, HX)), is(EQUAL_TO));
    assertThat(get(comparator.compare(LH, LH)), is(EQUAL_TO));
    assertThat(get(comparator.compare(LL, LL)), is(EQUAL_TO));
    assertThat(get(comparator.compare(LX, LX)), is(EQUAL_TO));

    assertThat(get(comparator.compare(HLLLLLLL, HLLLLLLL)), is(EQUAL_TO));
    assertThat(get(comparator.compare(HLLLLLLX, HLLLLLLX)), is(EQUAL_TO));
    assertThat(get(comparator.compare(LHLLLLLL, LHLLLLLL)), is(EQUAL_TO));
    assertThat(get(comparator.compare(LLLLLLLL, LLLLLLLL)), is(EQUAL_TO));
    assertThat(get(comparator.compare(LLLLLLLX, LLLLLLLX)), is(EQUAL_TO));
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

      assertThat(result2, is(result));

      // Now try with buffers sized to the array.
      o1b = ByteBuffer.wrap(o1);
      o2b = ByteBuffer.wrap(o2);
      final int result3 = c.compare(o1b, o2b);

      assertThat(result3, is(result));

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

  /** Converts an integer result code into its contractual meaning. */
  enum ComparatorResult {
    LESS_THAN,
    EQUAL_TO,
    GREATER_THAN;

    static ComparatorResult get(final int comparatorResult) {
      if (comparatorResult == 0) {
        return EQUAL_TO;
      }
      return comparatorResult < 0 ? LESS_THAN : GREATER_THAN;
    }
  }

  /** Interface that can test a {@link BufferProxy} <code>compare</code> method. */
  private interface ComparatorRunner {

    /**
     * Convert the passed byte arrays into the proxy's relevant buffer type and then invoke the
     * comparator.
     *
     * @param o1 lhs buffer content
     * @param o2 rhs buffer content
     * @return as per {@link Comparable}
     */
    int compare(byte[] o1, byte[] o2);
  }
}
