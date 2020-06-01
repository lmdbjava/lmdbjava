/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

import java.nio.ByteBuffer;

import jnr.ffi.Pointer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A buffer proxy backed by Agrona's {@link DirectBuffer}.
 *
 * <p>
 * This class requires {@link UnsafeAccess} and Agrona must be in the classpath.
 */
public final class DirectBufferProxy extends BufferProxy<DirectBuffer> {

  /**
   * The {@link MutableDirectBuffer} proxy. Guaranteed to never be null,
   * although a class initialization exception will occur if an attempt is made
   * to access this field when unsafe or Agrona is unavailable.
   */
  public static final BufferProxy<DirectBuffer> PROXY_DB
      = new DirectBufferProxy();

  /**
   * A thread-safe pool for a given length. If the buffer found is valid (ie not
   * of a negative length) then that buffer is used. If no valid buffer is
   * found, a new buffer is created.
   */
  private static final ThreadLocal<OneToOneConcurrentArrayQueue<DirectBuffer>> BUFFERS
      = withInitial(() -> new OneToOneConcurrentArrayQueue<>(16));

  private DirectBufferProxy() {
  }

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
    if (o1.equals(o2)) {
      return 0;
    }
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

  @Override
  protected DirectBuffer allocate() {
    final OneToOneConcurrentArrayQueue<DirectBuffer> q = BUFFERS.get();
    final DirectBuffer buffer = q.poll();

    if (buffer != null && buffer.capacity() >= 0) {
      return buffer;
    } else {
      final ByteBuffer bb = allocateDirect(0);
      return new UnsafeBuffer(bb);
    }
  }

  @Override
  protected int compare(final DirectBuffer o1, final DirectBuffer o2) {
    return compareBuff(o1, o2);
  }

  @Override
  protected void deallocate(final DirectBuffer buff) {
    final OneToOneConcurrentArrayQueue<DirectBuffer> q = BUFFERS.get();
    q.offer(buff);
  }

  @Override
  protected byte[] getBytes(final DirectBuffer buffer) {
    final byte[] dest = new byte[buffer.capacity()];
    buffer.getBytes(0, dest, 0, buffer.capacity());
    return dest;
  }

  @Override
  protected void in(final DirectBuffer buffer, final Pointer ptr,
                    final long ptrAddr) {
    final long addr = buffer.addressOffset();
    final long size = buffer.capacity();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
  }

  @Override
  protected void in(final DirectBuffer buffer, final int size, final Pointer ptr,
                    final long ptrAddr) {
    final long addr = buffer.addressOffset();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
  }

  @Override
  protected DirectBuffer out(final DirectBuffer buffer, final Pointer ptr,
                             final long ptrAddr) {
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    buffer.wrap(addr, (int) size);
    return buffer;
  }

}
