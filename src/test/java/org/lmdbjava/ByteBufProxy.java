/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
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

import io.netty.buffer.ByteBuf;
import static io.netty.buffer.PooledByteBufAllocator.DEFAULT;
import static java.lang.Class.forName;
import static java.lang.ThreadLocal.withInitial;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import jnr.ffi.Pointer;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

/**
 * A buffer proxy backed by Netty's {@link ByteBuf}.
 * <p>
 * This class requires {@link UnsafeAccess} and netty-buffer must be in the
 * classpath.
 */
public class ByteBufProxy extends BufferProxy<ByteBuf> {

  private static final long ADDRESS_OFFSET;

  /**
   * A thread-safe pool for a given length. If the buffer found is bigger then
   * the buffer in the pool creates a new buffer. If no buffer is found creates
   * a new buffer.
   */
  private static final ThreadLocal<ArrayDeque<ByteBuf>> BUFFERS
      = withInitial(() -> new ArrayDeque<>(16));

  private static final String FIELD_NAME_ADDRESS = "memoryAddress";
  private static final String FIELD_NAME_LENGTH = "length";
  private static final long LENGTH_OFFSET;
  private static final String NAME = "io.netty.buffer.PooledUnsafeDirectByteBuf";

  static {
    try {
      final Field address = findField(NAME, FIELD_NAME_ADDRESS);
      final Field length = findField(NAME, FIELD_NAME_LENGTH);
      ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
      LENGTH_OFFSET = UNSAFE.objectFieldOffset(length);
    } catch (SecurityException e) {
      throw new LmdbException("Field access error", e);
    }
  }

  static Field findField(final String c, final String name) {
    Class<?> clazz;
    try {
      clazz = forName(c);
    } catch (ClassNotFoundException e) {
      throw new LmdbException(c + " class unavailable", e);
    }
    do {
      try {
        final Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    } while (clazz != null);
    throw new LmdbException(name + " not found");
  }

  @Override
  protected ByteBuf allocate() {
    final ArrayDeque<ByteBuf> queue = BUFFERS.get();
    final ByteBuf buffer = queue.poll();

    if (buffer != null && buffer.capacity() >= 0) {
      return buffer;
    } else {
      return DEFAULT.directBuffer(0);
    }
  }

  @Override
  protected void deallocate(final ByteBuf buff) {
    final ArrayDeque<ByteBuf> queue = BUFFERS.get();
    if (!queue.offer(buff)) {
      buff.release();
    }
  }

  @Override
  protected void in(final ByteBuf buffer, final Pointer ptr, final long ptrAddr) {
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE,
                   buffer.writerIndex() - buffer.readerIndex());
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA,
                   buffer.memoryAddress() + buffer.readerIndex());
  }

  @Override
  protected void in(final ByteBuf buffer, final int size, final Pointer ptr,
                    final long ptrAddr) {
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE,
                   size);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA,
                   buffer.memoryAddress() + buffer.readerIndex());
  }

  @Override
  protected void out(final ByteBuf buffer, final Pointer ptr, final long ptrAddr) {
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, addr);
    UNSAFE.putLong(buffer, LENGTH_OFFSET, (int) size);
    buffer.readerIndex(0).writerIndex((int) size);
  }
}
