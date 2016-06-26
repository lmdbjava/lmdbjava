package org.lmdbjava;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import jnr.ffi.Pointer;

import java.lang.reflect.Field;

import static org.lmdbjava.UnsafeAccess.UNSAFE;

/**
 * A buffer proxy backed by Netty's {@link ByteBuf}.
 * <p>
 * This class requires {@link UnsafeAccess} and netty-buffer must be in the classpath.
 */
public class ByteBufProxy extends BufferProxy<ByteBuf> {
  private static final String FIELD_NAME_ADDRESS = "memoryAddress";
  private static final String FIELD_NAME_LENGTH = "length";

  private static final long ADDRESS_OFFSET;
  private static final long LENGTH_OFFSET;

  static {
    try {
      final Field address = findField("io.netty.buffer.PooledUnsafeDirectByteBuf", FIELD_NAME_ADDRESS);
      final Field length = findField("io.netty.buffer.PooledUnsafeDirectByteBuf", FIELD_NAME_LENGTH);
      ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
      LENGTH_OFFSET = UNSAFE.objectFieldOffset(length);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  static Field findField(final String c, final String name) {
    Class<?> clazz;
    try {
      clazz = Class.forName(c);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
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
    throw new RuntimeException(name + " not found");
  }

  @Override
  protected ByteBuf allocate(int bytes) {
    return PooledByteBufAllocator.DEFAULT.directBuffer(bytes);
  }

  @Override
  protected void deallocate(ByteBuf buff) {
    buff.release();
  }

  @Override
  protected void in(ByteBuf buffer, Pointer ptr, long ptrAddr) {
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.capacity());
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, buffer.memoryAddress());
  }

  @Override
  protected void out(ByteBuf buffer, Pointer ptr, long ptrAddr) {
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, addr);
    UNSAFE.putLong(buffer, LENGTH_OFFSET, (int) size);
    buffer.readerIndex(0).writerIndex((int) size);
  }
}
