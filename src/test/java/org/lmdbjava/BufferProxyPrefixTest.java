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
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufProxy.PROXY_NETTY;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for the prefix helpers {@link BufferProxy#containsPrefix} and {@link
 * BufferProxy#incrementLeastSignificantByte} across every {@link BufferProxy} implementation.
 *
 * <p>These methods back the (reverse) prefix iteration added for gh-269 but had no direct unit
 * coverage. Buffers are built in big-endian order (the default), so the "least significant byte" is
 * the last byte, matching the byte-array behaviour.
 */
final class BufferProxyPrefixTest {

  static Stream<Arguments> proxies() {
    return Stream.of(
        Arguments.argumentSet("ByteArrayProxy", new ByteArrayAdapter()),
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
  void containsPrefixMatches(final Adapter adapter) {
    // A shorter prefix that matches the leading bytes.
    assertThat(adapter.containsPrefix(bytes(1, 2, 3, 4), bytes(1, 2))).isTrue();
    // An identical buffer contains itself as a prefix.
    assertThat(adapter.containsPrefix(bytes(1, 2, 3, 4), bytes(1, 2, 3, 4))).isTrue();
    // The empty prefix is contained by anything (including the empty buffer).
    assertThat(adapter.containsPrefix(bytes(1, 2, 3, 4), bytes())).isTrue();
    assertThat(adapter.containsPrefix(bytes(), bytes())).isTrue();
    // Unsigned byte comparison: 0x80 must be treated as greater than 0x01, not negative.
    assertThat(adapter.containsPrefix(bytes(0x80, 0x00), bytes(0x80))).isTrue();
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void containsPrefixRejects(final Adapter adapter) {
    // Same length, differing byte.
    assertThat(adapter.containsPrefix(bytes(1, 2, 3, 4), bytes(1, 3))).isFalse();
    // A prefix longer than the buffer can never be contained.
    assertThat(adapter.containsPrefix(bytes(1, 2), bytes(1, 2, 3))).isFalse();
    assertThat(adapter.containsPrefix(bytes(), bytes(1))).isFalse();
    // Leading byte differs.
    assertThat(adapter.containsPrefix(bytes(2, 2, 3, 4), bytes(1))).isFalse();
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void incrementSimple(final Adapter adapter) {
    assertThat(adapter.increment(bytes(5))).containsExactly(bytes(6));
    assertThat(adapter.increment(bytes(0))).containsExactly(bytes(1));
    // Only the least significant (last) byte changes when it is not 0xFF.
    assertThat(adapter.increment(bytes(0x12, 0x34))).containsExactly(bytes(0x12, 0x35));
    assertThat(adapter.increment(bytes(0x12, 0xFE))).containsExactly(bytes(0x12, 0xFF));
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void incrementTruncatesTrailing0xFfToTightSuccessor(final Adapter adapter) {
    // When the least significant byte(s) are 0xFF, the next non-0xFF byte to the left is
    // incremented and the trailing 0xFF bytes are dropped, giving the tight prefix successor.
    // e.g. {0x01,0xFF} -> {0x02} (not {0x02,0xFF}); this prevents reverse prefix iteration from
    // over-shooting onto an unrelated higher key.
    assertThat(adapter.increment(bytes(0x01, 0xFF))).containsExactly(bytes(0x02));
    assertThat(adapter.increment(bytes(0x01, 0xFF, 0xFF))).containsExactly(bytes(0x02));
    assertThat(adapter.increment(bytes(0x12, 0xFF))).containsExactly(bytes(0x13));
  }

  @ParameterizedTest
  @MethodSource("proxies")
  void incrementReturnsNullWhenNoUpperBound(final Adapter adapter) {
    // All bytes already at max unsigned value: no "one bigger" buffer exists.
    assertThat(adapter.increment(bytes(0xFF))).isNull();
    assertThat(adapter.increment(bytes(0xFF, 0xFF))).isNull();
    // An empty buffer has no byte to increment.
    assertThat(adapter.increment(bytes())).isNull();
  }

  /** Abstracts buffer construction so the same assertions run against every proxy type. */
  interface Adapter {

    boolean containsPrefix(byte[] buffer, byte[] prefix);

    /**
     * @return the incremented bytes, or null if the proxy returned null.
     */
    byte[] increment(byte[] buffer);
  }

  private static final class ByteArrayAdapter implements Adapter {

    @Override
    public boolean containsPrefix(final byte[] buffer, final byte[] prefix) {
      return PROXY_BA.containsPrefix(buffer, prefix);
    }

    @Override
    public byte[] increment(final byte[] buffer) {
      return PROXY_BA.incrementLeastSignificantByte(buffer);
    }
  }

  private static final class ByteBufferAdapter implements Adapter {

    private static ByteBuffer bb(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes);
      buffer.flip();
      return buffer;
    }

    @Override
    public boolean containsPrefix(final byte[] buffer, final byte[] prefix) {
      return PROXY_OPTIMAL.containsPrefix(bb(buffer), bb(prefix));
    }

    @Override
    public byte[] increment(final byte[] buffer) {
      final ByteBuffer result = PROXY_OPTIMAL.incrementLeastSignificantByte(bb(buffer));
      if (result == null) {
        return null;
      }
      final ByteBuffer dup = result.duplicate();
      final byte[] out = new byte[dup.remaining()];
      dup.get(out);
      return out;
    }
  }

  private static final class DirectBufferAdapter implements Adapter {

    private static UnsafeBuffer db(final byte[] bytes) {
      // Wrap a direct ByteBuffer so DirectBufferProxy can read back its byteBuffer().
      final ByteBuffer backing = ByteBuffer.allocateDirect(bytes.length);
      backing.put(bytes);
      backing.flip();
      return new UnsafeBuffer(backing);
    }

    @Override
    public boolean containsPrefix(final byte[] buffer, final byte[] prefix) {
      return PROXY_DB.containsPrefix(db(buffer), db(prefix));
    }

    @Override
    public byte[] increment(final byte[] buffer) {
      final org.agrona.DirectBuffer result = PROXY_DB.incrementLeastSignificantByte(db(buffer));
      if (result == null) {
        return null;
      }
      final byte[] out = new byte[result.capacity()];
      result.getBytes(0, out);
      return out;
    }
  }

  private static final class NettyAdapter implements Adapter {

    private static ByteBuf nb(final byte[] bytes) {
      // wrappedBuffer gives capacity == content length, which the Netty proxy relies on.
      return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public boolean containsPrefix(final byte[] buffer, final byte[] prefix) {
      final ByteBuf b = nb(buffer);
      final ByteBuf p = nb(prefix);
      try {
        return PROXY_NETTY.containsPrefix(b, p);
      } finally {
        b.release();
        p.release();
      }
    }

    @Override
    public byte[] increment(final byte[] buffer) {
      final ByteBuf b = nb(buffer);
      try {
        final ByteBuf result = PROXY_NETTY.incrementLeastSignificantByte(b);
        if (result == null) {
          return null;
        }
        try {
          final byte[] out = new byte[result.capacity()];
          result.getBytes(0, out);
          return out;
        } finally {
          result.release();
        }
      } finally {
        b.release();
      }
    }
  }
}
