/*
 * Copyright 2016 The LmdbJava Open Source Project.
 *
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
 */
package org.lmdbjava;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import jnr.ffi.Pointer;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

/**
 * {@link ByteBuffer}-based proxy.
 * <p>
 * There are two concrete {@link ByteBuffer} cursor implementations available:
 * <ul>
 * <li>A "fast" implementation: {@link UnsafeProxy}</li>
 * <li>A "safe" implementation: {@link ReflectiveProxy}</li>
 * </ul>
 * <p>
 * Users nominate which implementation they prefer by referencing the
 * {@link #FACTORY_OPTIMAL} or {@link #FACTORY_SAFE} field when invoking
 * {@link Dbi#openCursor(org.lmdbjava.Txn, org.lmdbjava.CursorFactory)}.
 */
public final class ByteBufferProxy {

  /**
   * The fastest {@link ByteBuffer} proxy that is available on this platform.
   * This will always be the same instance as {@link #FACTORY_SAFE} if the
   * {@link UnsafeAccess#DISABLE_UNSAFE_PROP} has been set to <code>true</code>
   * or {@link UnsafeAccess} is unavailable. Guaranteed to never be null.
   */
  public static final BufferProxy<ByteBuffer> PROXY_OPTIMAL;

  /**
   * The safe, reflective {@link ByteBuffer} proxy for this system. Guaranteed
   * to never be null.
   */
  public static final BufferProxy<ByteBuffer> PROXY_SAFE;
  private static final String FIELD_NAME_ADDRESS = "address";
  private static final String FIELD_NAME_CAPACITY = "capacity";

  static {
    PROXY_SAFE = new ReflectiveProxy();
    PROXY_OPTIMAL = getProxyOptimal();
  }

  private static BufferProxy<ByteBuffer> getProxyOptimal() {
    try {
      return new UnsafeProxy();
    } catch (Throwable e) {
      return PROXY_SAFE;
    }
  }

  static Field findField(final Class<?> c, final String name) {
    Class<?> clazz = c;
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

  private ByteBufferProxy() {
  }

  /**
   * A proxy that uses Java reflection to modify byte buffer fields, and
   * official JNR-FFF methods to manipulate native pointers.
   */
  private static final class ReflectiveProxy implements BufferProxy<ByteBuffer> {

    private static final Field ADDRESS_FIELD;
    private static final Field CAPACITY_FIELD;

    static {
      ADDRESS_FIELD = findField(Buffer.class, FIELD_NAME_ADDRESS);
      CAPACITY_FIELD = findField(Buffer.class, FIELD_NAME_CAPACITY);
    }

    @Override
    public ByteBuffer allocate(int bytes) {
      return allocateDirect(bytes);
    }

    @Override
    public void dirty(ByteBuffer roBuffer, Pointer ptr, long ptrAddr) {
      final long addr = ptr.getLong(STRUCT_FIELD_OFFSET_DATA);
      final long size = ptr.getLong(STRUCT_FIELD_OFFSET_SIZE);
      try {
        ADDRESS_FIELD.set(roBuffer, addr);
        CAPACITY_FIELD.set(roBuffer, (int) size);
      } catch (IllegalArgumentException | IllegalAccessException ex) {
        throw new RuntimeException("Cannot modify buffer", ex);
      }
      roBuffer.clear();
    }

    @Override
    public void set(ByteBuffer buffer, Pointer ptr, long ptrAddr) {
      final long addr = ((sun.nio.ch.DirectBuffer) buffer).address();
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.capacity());
      ptr.putLong(STRUCT_FIELD_OFFSET_DATA, addr);
    }

  }

  /**
   * A proxy that uses Java's "unsafe" class to directly manipulate byte buffer
   * fields and JNR-FFF allocated memory pointers.
   */
  private static final class UnsafeProxy implements BufferProxy<ByteBuffer> {

    static final long ADDRESS_OFFSET;
    static final long CAPACITY_OFFSET;

    static {
      try {
        final Field address = findField(Buffer.class, FIELD_NAME_ADDRESS);
        final Field capacity = findField(Buffer.class, FIELD_NAME_CAPACITY);
        ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
        CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity);
      } catch (SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public ByteBuffer allocate(int bytes) {
      return allocateDirect(bytes);
    }

    @Override
    public void dirty(ByteBuffer roBuffer, Pointer ptr, long ptrAddr) {
      final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
      final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
      UNSAFE.putLong(roBuffer, ADDRESS_OFFSET, addr);
      UNSAFE.putInt(roBuffer, CAPACITY_OFFSET, (int) size);
      roBuffer.clear();
    }

    @Override
    public void set(ByteBuffer buffer, Pointer ptr, long ptrAddr) {
      final long addr = ((sun.nio.ch.DirectBuffer) buffer).address();
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.capacity());
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    }

  }
}
