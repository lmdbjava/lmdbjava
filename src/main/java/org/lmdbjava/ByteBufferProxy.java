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

import static java.lang.Long.reverseBytes;
import static java.lang.ThreadLocal.withInitial;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import java.util.ArrayDeque;
import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

/**
 * {@link ByteBuffer}-based proxy.
 *
 * <p>
 * There are two concrete {@link ByteBuffer} proxy implementations available:
 * <ul>
 * <li>A "fast" implementation: {@link UnsafeProxy}</li>
 * <li>A "safe" implementation: {@link ReflectiveProxy}</li>
 * </ul>
 *
 * <p>
 * Users nominate which implementation they prefer by referencing the
 * {@link #PROXY_OPTIMAL} or {@link #PROXY_SAFE} field when invoking
 * {@link Env#create(org.lmdbjava.BufferProxy)}.
 */
@SuppressWarnings("PMD.AvoidCatchingGenericException")
public final class ByteBufferProxy {

  /**
   * The fastest {@link ByteBuffer} proxy that is available on this platform.
   * This will always be the same instance as {@link #PROXY_SAFE} if the
   * {@link UnsafeAccess#DISABLE_UNSAFE_PROP} has been set to <code>true</code>
   * and/or {@link UnsafeAccess} is unavailable. Guaranteed to never be null.
   */
  public static final BufferProxy<ByteBuffer> PROXY_OPTIMAL;

  /**
   * The safe, reflective {@link ByteBuffer} proxy for this system. Guaranteed
   * to never be null.
   */
  public static final BufferProxy<ByteBuffer> PROXY_SAFE;

  static {
    PROXY_SAFE = new ReflectiveProxy();
    PROXY_OPTIMAL = getProxyOptimal();
  }

  private ByteBufferProxy() {
  }

  private static BufferProxy<ByteBuffer> getProxyOptimal() {
    try {
      return new UnsafeProxy();
    } catch (final RuntimeException e) {
      return PROXY_SAFE;
    }
  }

  /**
   * The buffer must be a direct buffer (not heap allocated).
   */
  public static final class BufferMustBeDirectException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public BufferMustBeDirectException() {
      super("The buffer must be a direct buffer (not heap allocated");
    }
  }

  /**
   * Provides {@link ByteBuffer} pooling and address resolution for concrete
   * {@link BufferProxy} implementations.
   */
  abstract static class AbstractByteBufferProxy extends BufferProxy<ByteBuffer> {

    protected static final String FIELD_NAME_ADDRESS = "address";
    protected static final String FIELD_NAME_CAPACITY = "capacity";

    /**
     * A thread-safe pool for a given length. If the buffer found is valid (ie
     * not of a negative length) then that buffer is used. If no valid buffer is
     * found, a new buffer is created.
     */
    private static final ThreadLocal<ArrayDeque<ByteBuffer>> BUFFERS
        = withInitial(() -> new ArrayDeque<>(16));

    /**
     * Lexicographically compare two buffers.
     *
     * @param o1 left operand (required)
     * @param o2 right operand (required)
     * @return as specified by {@link Comparable} interface
     */
    @SuppressWarnings({"checkstyle:ReturnCount", "PMD.CyclomaticComplexity"})
    public static int compareBuff(final ByteBuffer o1, final ByteBuffer o2) {
      requireNonNull(o1);
      requireNonNull(o2);
      if (o1.equals(o2)) {
        return 0;
      }
      final int minLength = Math.min(o1.limit(), o2.limit());
      final int minWords = minLength / Long.BYTES;

      final boolean reverse1 = o1.order() == LITTLE_ENDIAN;
      final boolean reverse2 = o1.order() == LITTLE_ENDIAN;
      for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
        final long lw = reverse1 ? reverseBytes(o1.getLong(i)) : o1.getLong(i);
        final long rw = reverse2 ? reverseBytes(o2.getLong(i)) : o2.getLong(i);
        final int diff = Long.compareUnsigned(lw, rw);
        if (diff != 0) {
          return diff;
        }
      }

      for (int i = minWords * Long.BYTES; i < minLength; i++) {
        final int lw = Byte.toUnsignedInt(o1.get(i));
        final int rw = Byte.toUnsignedInt(o2.get(i));
        final int result = Integer.compareUnsigned(lw, rw);
        if (result != 0) {
          return result;
        }
      }

      return o1.capacity() - o2.capacity();
    }

    static Field findField(final Class<?> c, final String name) {
      Class<?> clazz = c;
      do {
        try {
          final Field field = clazz.getDeclaredField(name);
          field.setAccessible(true);
          return field;
        } catch (final NoSuchFieldException e) {
          clazz = clazz.getSuperclass();
        }
      } while (clazz != null);
      throw new LmdbException(name + " not found");
    }

    protected final long address(final ByteBuffer buffer) {
      if (SHOULD_CHECK && !buffer.isDirect()) {
        throw new BufferMustBeDirectException();
      }
      return ((sun.nio.ch.DirectBuffer) buffer).address() + buffer.position();
    }

    @Override
    protected final ByteBuffer allocate() {
      final ArrayDeque<ByteBuffer> queue = BUFFERS.get();
      final ByteBuffer buffer = queue.poll();

      if (buffer != null && buffer.capacity() >= 0) {
        return buffer;
      } else {
        return allocateDirect(0);
      }
    }

    @Override
    protected final int compare(final ByteBuffer o1, final ByteBuffer o2) {
      return compareBuff(o1, o2);
    }

    @Override
    protected final void deallocate(final ByteBuffer buff) {
      buff.order(BIG_ENDIAN);
      final ArrayDeque<ByteBuffer> queue = BUFFERS.get();
      queue.offer(buff);
    }

    @Override
    protected byte[] getBytes(final ByteBuffer buffer) {
      final byte[] dest = new byte[buffer.limit()];
      buffer.get(dest, 0, buffer.limit());
      return dest;
    }

  }

  /**
   * A proxy that uses Java reflection to modify byte buffer fields, and
   * official JNR-FFF methods to manipulate native pointers.
   */
  private static final class ReflectiveProxy extends AbstractByteBufferProxy {

    private static final Field ADDRESS_FIELD;
    private static final Field CAPACITY_FIELD;

    static {
      ADDRESS_FIELD = findField(Buffer.class, FIELD_NAME_ADDRESS);
      CAPACITY_FIELD = findField(Buffer.class, FIELD_NAME_CAPACITY);
    }

    @Override
    protected void in(final ByteBuffer buffer, final Pointer ptr,
                      final long ptrAddr) {
      ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, address(buffer));
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.remaining());
    }

    @Override
    protected void in(final ByteBuffer buffer, final int size, final Pointer ptr,
                      final long ptrAddr) {
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, size);
      ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, address(buffer));
    }

    @Override
    protected ByteBuffer out(final ByteBuffer buffer, final Pointer ptr,
                             final long ptrAddr) {
      final long addr = ptr.getAddress(STRUCT_FIELD_OFFSET_DATA);
      final long size = ptr.getLong(STRUCT_FIELD_OFFSET_SIZE);
      try {
        ADDRESS_FIELD.set(buffer, addr);
        CAPACITY_FIELD.set(buffer, (int) size);
      } catch (final IllegalArgumentException | IllegalAccessException e) {
        throw new LmdbException("Cannot modify buffer", e);
      }
      buffer.clear();
      return buffer;
    }

  }

  /**
   * A proxy that uses Java's "unsafe" class to directly manipulate byte buffer
   * fields and JNR-FFF allocated memory pointers.
   */
  private static final class UnsafeProxy extends AbstractByteBufferProxy {

    private static final long ADDRESS_OFFSET;
    private static final long CAPACITY_OFFSET;

    static {
      try {
        final Field address = findField(Buffer.class, FIELD_NAME_ADDRESS);
        final Field capacity = findField(Buffer.class, FIELD_NAME_CAPACITY);
        ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
        CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity);
      } catch (final SecurityException e) {
        throw new LmdbException("Field access error", e);
      }
    }

    @Override
    protected void in(final ByteBuffer buffer, final Pointer ptr,
                      final long ptrAddr) {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.remaining());
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address(buffer));
    }

    @Override
    protected void in(final ByteBuffer buffer, final int size, final Pointer ptr,
                      final long ptrAddr) {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address(buffer));
    }

    @Override
    protected ByteBuffer out(final ByteBuffer buffer, final Pointer ptr,
                             final long ptrAddr) {
      final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
      final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
      UNSAFE.putLong(buffer, ADDRESS_OFFSET, addr);
      UNSAFE.putInt(buffer, CAPACITY_OFFSET, (int) size);
      buffer.clear();
      return buffer;
    }
  }

}
