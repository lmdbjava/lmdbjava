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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Focused tests for the {@code compareAsIntegerKeys} static comparators on each integer-key capable
 * proxy. {@link ComparatorIntegerKeyTest} already covers the numeric ordering semantics; this test
 * targets the previously-uncovered branches: the length-mismatch guard and the fallback to
 * lexicographic comparison when the key length is not 4 or 8 bytes.
 */
final class CompareAsIntegerKeysTest {

  static Stream<Arguments> proxies() {
    return Stream.of(
        Arguments.argumentSet("ByteBufferProxy", new ByteBufferAdapter()),
        Arguments.argumentSet("DirectBufferProxy", new DirectBufferAdapter()),
        Arguments.argumentSet("ByteBufProxy", new NettyAdapter()));
  }

  private static byte[] bytes(final int... values) {
    final byte[] array = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      array[i] = (byte) values[i];
    }
    return array;
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void lengthMismatchThrows(final Adapter adapter) {
    assertThatThrownBy(() -> adapter.compare(bytes(0, 0, 0, 1), bytes(0, 0, 0, 0, 0, 0, 0, 1)))
        .isInstanceOf(RuntimeException.class);
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void equalBytesCompareEqual(final Adapter adapter) {
    assertThat(adapter.compare(bytes(1, 2, 3, 4), bytes(1, 2, 3, 4))).isZero();
    assertThat(adapter.compare(bytes(1, 2, 3, 4, 5, 6, 7, 8), bytes(1, 2, 3, 4, 5, 6, 7, 8)))
        .isZero();
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void nonIntegerLengthFallsBackToLexicographic(final Adapter adapter) {
    // 2-byte keys are neither 4 nor 8 bytes, so the comparator falls back to unsigned
    // lexicographic.
    assertThat(adapter.compare(bytes(0x00, 0x01), bytes(0x00, 0x02))).isNegative();
    assertThat(adapter.compare(bytes(0x00, 0x02), bytes(0x00, 0x01))).isPositive();
    assertThat(adapter.compare(bytes(0x12, 0x34), bytes(0x12, 0x34))).isZero();
    // A single high-bit byte must be treated as unsigned (0x80 > 0x01), not as a negative byte.
    assertThat(adapter.compare(bytes(0x80), bytes(0x01))).isPositive();
  }

  /** Normalises the differing static comparator signatures to a single byte[] based call. */
  interface Adapter {
    int compare(byte[] a, byte[] b);
  }

  private static final class ByteBufferAdapter implements Adapter {

    private static ByteBuffer bb(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes);
      buffer.flip();
      return buffer;
    }

    @Override
    public int compare(final byte[] a, final byte[] b) {
      return ByteBufferProxy.AbstractByteBufferProxy.compareAsIntegerKeys(bb(a), bb(b));
    }
  }

  private static final class DirectBufferAdapter implements Adapter {

    @Override
    public int compare(final byte[] a, final byte[] b) {
      return DirectBufferProxy.compareAsIntegerKeys(new UnsafeBuffer(a), new UnsafeBuffer(b));
    }
  }

  private static final class NettyAdapter implements Adapter {

    @Override
    public int compare(final byte[] a, final byte[] b) {
      final ByteBuf ba = Unpooled.wrappedBuffer(a);
      final ByteBuf bb = Unpooled.wrappedBuffer(b);
      try {
        return ByteBufProxy.compareAsIntegerKeys(ba, bb);
      } finally {
        ba.release();
        bb.release();
      }
    }
  }
}
