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

import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import jnr.ffi.Pointer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

/**
 * A buffer proxy that backed by Agrona's {@link MutableDirectBuffer}.
 * <p>
 * This class requires {@link UnsafeAccess} and Agrona must be in the classpath.
 */
public final class MutableDirectBufferProxy implements
    BufferProxy<MutableDirectBuffer> {

  /**
   * The {@link MutableDirectBuffer} proxy factory. Guaranteed to never be null,
   * although a class initialization exception will occur if unsafe is
   * unavailable or Agrona is not in the classpath.
   */
  public static final BufferProxyFactory<MutableDirectBuffer> FACTORY_MDB
      = new MutableDirectBufferProxyFactory();

  private static MutableDirectBuffer alloc(int bytes) {
    ByteBuffer bb = allocateDirect(bytes);
    return new UnsafeBuffer(bb);
  }

  @Override
  public MutableDirectBuffer allocate(int bytes) {
    return alloc(bytes);
  }

  @Override
  public void dirty(MutableDirectBuffer roBuffer, Pointer ptr,
                    long ptrAddr) {
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    roBuffer.wrap(addr, (int) size);
  }

  @Override
  public void set(MutableDirectBuffer buffer, Pointer ptr, long ptrAddr) {
    final long addr = buffer.addressOffset();
    final long size = buffer.capacity();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
  }

  static final class MutableDirectBufferProxyFactory implements
      BufferProxyFactory<MutableDirectBuffer> {

    @Override
    public BufferProxy<MutableDirectBuffer> create() {
      return new MutableDirectBufferProxy();
    }
  }

}
