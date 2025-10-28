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
import static java.lang.Class.forName;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.lang.reflect.Field;
import java.util.Comparator;
import jnr.ffi.Pointer;

/**
 * A buffer proxy backed by Netty's {@link ByteBuf}.
 *
 * <p>This class requires {@link UnsafeAccess} and netty-buffer must be in the classpath.
 */
public final class ByteBufProxy extends BufferProxy<ByteBuf> {

  /**
   * A proxy for using Netty {@link ByteBuf}. Guaranteed to never be null, although a class
   * initialization exception will occur if an attempt is made to access this field when Netty is
   * unavailable.
   */
  public static final BufferProxy<ByteBuf> PROXY_NETTY = new ByteBufProxy();

  private static final int BUFFER_RETRIES = 10;
  private static final String FIELD_NAME_ADDRESS = "memoryAddress";
  private static final String FIELD_NAME_LENGTH = "length";
  private static final String NAME = "io.netty.buffer.PooledUnsafeDirectByteBuf";
  private static final Comparator<ByteBuf> comparator =
      (o1, o2) -> {
        requireNonNull(o1);
        requireNonNull(o2);

        return o1.compareTo(o2);
      };
  private final long lengthOffset;
  private final long addressOffset;

  private final PooledByteBufAllocator nettyAllocator;

  private ByteBufProxy() {
    this(DEFAULT);
  }

  /**
   * Constructs a buffer proxy for use with Netty.
   *
   * @param allocator the Netty allocator to obtain the {@link ByteBuf} from
   */
  public ByteBufProxy(final PooledByteBufAllocator allocator) {
    super();
    this.nettyAllocator = allocator;

    try {
      final ByteBuf initBuf = this.allocate();
      initBuf.release();
      final Field address = findField(NAME, FIELD_NAME_ADDRESS);
      final Field length = findField(NAME, FIELD_NAME_LENGTH);
      addressOffset = UNSAFE.objectFieldOffset(address);
      lengthOffset = UNSAFE.objectFieldOffset(length);
    } catch (final SecurityException e) {
      throw new LmdbException("Field access error", e);
    }
  }

  static Field findField(final String c, final String name) {
    Class<?> clazz;
    try {
      clazz = forName(c);
    } catch (final ClassNotFoundException e) {
      throw new LmdbException(c + " class unavailable", e);
    }
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

  @Override
  protected ByteBuf allocate() {
    for (int i = 0; i < BUFFER_RETRIES; i++) {
      final ByteBuf bb = nettyAllocator.directBuffer();
      if (NAME.equals(bb.getClass().getName())) {
        return bb;
      } else {
        bb.release();
      }
    }
    throw new IllegalStateException("Netty buffer must be " + NAME);
  }

  @Override
  public Comparator<ByteBuf> getComparator(final DbiFlagSet dbiFlagSet) {
    return comparator;
  }

  //  @Override
//  public Comparator<ByteBuf> getSignedComparator() {
//    return comparator;
//  }
//
//  @Override
//  public Comparator<ByteBuf> getUnsignedComparator() {
//    return comparator;
//  }

  @Override
  protected void deallocate(final ByteBuf buff) {
    buff.release();
  }

  @Override
  protected byte[] getBytes(final ByteBuf buffer) {
    final byte[] dest = new byte[buffer.capacity()];
    buffer.getBytes(0, dest);
    return dest;
  }

  @Override
  protected Pointer in(final ByteBuf buffer, final Pointer ptr) {
    final long ptrAddr = ptr.address();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.writerIndex() - buffer.readerIndex());
    UNSAFE.putLong(
        ptrAddr + STRUCT_FIELD_OFFSET_DATA, buffer.memoryAddress() + buffer.readerIndex());
    return null;
  }

  @Override
  protected Pointer in(final ByteBuf buffer, final int size, final Pointer ptr) {
    final long ptrAddr = ptr.address();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
    UNSAFE.putLong(
        ptrAddr + STRUCT_FIELD_OFFSET_DATA, buffer.memoryAddress() + buffer.readerIndex());
    return null;
  }

  @Override
  protected ByteBuf out(final ByteBuf buffer, final Pointer ptr) {
    final long ptrAddr = ptr.address();
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    UNSAFE.putLong(buffer, addressOffset, addr);
    UNSAFE.putInt(buffer, lengthOffset, (int) size);
    buffer.clear().writerIndex((int) size);
    return buffer;
  }
}
