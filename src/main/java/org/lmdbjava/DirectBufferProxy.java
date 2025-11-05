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

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Comparator;
import jnr.ffi.Pointer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A buffer proxy backed by Agrona's {@link DirectBuffer}.
 *
 * <p>This class requires {@link UnsafeAccess} and Agrona must be in the classpath.
 */
public final class DirectBufferProxy extends BufferProxy<DirectBuffer> {
  private static final Comparator<DirectBuffer> signedComparator =
      (o1, o2) -> {
        requireNonNull(o1);
        requireNonNull(o2);

        return o1.compareTo(o2);
      };
  private static final Comparator<DirectBuffer> unsignedComparator = DirectBufferProxy::compareBuff;

  /**
   * The {@link MutableDirectBuffer} proxy. Guaranteed to never be null, although a class
   * initialization exception will occur if an attempt is made to access this field when unsafe or
   * Agrona is unavailable.
   */
  public static final BufferProxy<DirectBuffer> PROXY_DB = new DirectBufferProxy();

  /**
   * A thread-safe pool for a given length. If the buffer found is valid (ie not of a negative
   * length) then that buffer is used. If no valid buffer is found, a new buffer is created.
   */
  private static final ThreadLocal<ArrayDeque<DirectBuffer>> BUFFERS =
      withInitial(() -> new ArrayDeque<>(16));

  private DirectBufferProxy() {}

  /**
   * Lexicographically compare two buffers.
   *
   * @param o1 left operand (required)
   * @param o2 right operand (required)
   * @return as specified by {@link Comparable} interface
   */
  public static int compareBuff(final DirectBuffer o1, final DirectBuffer o2) {
    requireNonNull(o1);
    requireNonNull(o2);

    final int minLength = Math.min(o1.capacity(), o2.capacity());
    final int minWords = minLength / Long.BYTES;

    for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
      final long lw = o1.getLong(i, BIG_ENDIAN);
      final long rw = o2.getLong(i, BIG_ENDIAN);
      final int diff = Long.compareUnsigned(lw, rw);
      if (diff != 0) {
        return diff;
      }
    }

    for (int i = minWords * Long.BYTES; i < minLength; i++) {
      final int lw = Byte.toUnsignedInt(o1.getByte(i));
      final int rw = Byte.toUnsignedInt(o2.getByte(i));
      final int result = Integer.compareUnsigned(lw, rw);
      if (result != 0) {
        return result;
      }
    }

    return o1.capacity() - o2.capacity();
  }

  /**
   * Lexicographically compare two buffers up to a shared length.
   *
   * @param o1 left operand (required)
   * @param o2 right operand (required)
   * @param minLength length to compare
   * @return as specified by {@link Comparable} interface
   */
  public static int compareBuff(final DirectBuffer o1, final DirectBuffer o2, final int minLength) {
    requireNonNull(o1);
    requireNonNull(o2);
    final int minWords = minLength / Long.BYTES;

    for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
      final long lw = o1.getLong(i, BIG_ENDIAN);
      final long rw = o2.getLong(i, BIG_ENDIAN);
      final int diff = Long.compareUnsigned(lw, rw);
      if (diff != 0) {
        return diff;
      }
    }

    for (int i = minWords * Long.BYTES; i < minLength; i++) {
      final int lw = Byte.toUnsignedInt(o1.getByte(i));
      final int rw = Byte.toUnsignedInt(o2.getByte(i));
      final int result = Integer.compareUnsigned(lw, rw);
      if (result != 0) {
        return result;
      }
    }

    return 0;
  }

  @Override
  protected DirectBuffer allocate() {
    final ArrayDeque<DirectBuffer> q = BUFFERS.get();
    final DirectBuffer buffer = q.poll();

    if (buffer != null && buffer.capacity() >= 0) {
      return buffer;
    } else {
      final ByteBuffer bb = allocateDirect(0);
      return new UnsafeBuffer(bb);
    }
  }

  @Override
  protected Comparator<DirectBuffer> getSignedComparator() {
    return signedComparator;
  }

  @Override
  protected Comparator<DirectBuffer> getUnsignedComparator() {
    return unsignedComparator;
  }

  @Override
  protected void deallocate(final DirectBuffer buff) {
    final ArrayDeque<DirectBuffer> q = BUFFERS.get();
    q.offer(buff);
  }

  @Override
  protected byte[] getBytes(final DirectBuffer buffer) {
    final byte[] dest = new byte[buffer.capacity()];
    buffer.getBytes(0, dest, 0, buffer.capacity());
    return dest;
  }

  @Override
  protected Pointer in(final DirectBuffer buffer, final Pointer ptr) {
    final long ptrAddr = ptr.address();
    final long addr = buffer.addressOffset();
    final long size = buffer.capacity();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
    return null;
  }

  @Override
  protected Pointer in(final DirectBuffer buffer, final int size, final Pointer ptr) {
    final long ptrAddr = ptr.address();
    final long addr = buffer.addressOffset();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
    return null;
  }

  @Override
  protected DirectBuffer out(final DirectBuffer buffer, final Pointer ptr) {
    final long ptrAddr = ptr.address();
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    buffer.wrap(addr, (int) size);
    return buffer;
  }

  @Override
  boolean containsPrefix(final DirectBuffer buffer, final DirectBuffer prefixBuffer) {
    if (buffer.capacity() < prefixBuffer.capacity()) {
      return false;
    }

    // We don't care about signed or unsigned since we are checking for equality.
    return compareBuff(buffer, prefixBuffer, prefixBuffer.capacity()) == 0;
  }

  @Override
  DirectBuffer incrementLeastSignificantByte(final DirectBuffer directBuffer) {
    if (directBuffer == null || directBuffer.capacity() == 0) {
      return null;
    }

    final ByteBuffer buffer = directBuffer.byteBuffer();
    if (LITTLE_ENDIAN.equals(buffer.order())) {
      // Start from the least significant byte (closest to start)
      for (int i = buffer.position(); i < buffer.limit(); i++) {
        final byte b = buffer.get(i);

        // Check if byte is not at max unsigned value (0xFF = 255 = -1 in signed)
        if (b != (byte) 0xFF) {
          final ByteBuffer oneBigger = ByteBuffer.allocateDirect(buffer.remaining());
          oneBigger.put(buffer.duplicate());
          oneBigger.flip();
          oneBigger.put(i - buffer.position(), (byte) (b + 1));
          return new UnsafeBuffer(oneBigger);
        }
      }

    } else {
      // Start from the least significant byte (closest to limit)
      for (int i = buffer.limit() - 1; i >= buffer.position(); i--) {
        final byte b = buffer.get(i);

        // Check if byte is not at max unsigned value (0xFF = 255 = -1 in signed)
        if (b != (byte) 0xFF) {
          final ByteBuffer oneBigger = ByteBuffer.allocateDirect(buffer.remaining());
          oneBigger.put(buffer.duplicate());
          oneBigger.flip();
          oneBigger.put(i - buffer.position(), (byte) (b + 1));
          return new UnsafeBuffer(oneBigger);
        }
      }
    }

    // All bytes are at maximum value
    return null;
  }
}
