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
 * A buffer proxy backed by Agrona's {@link MutableDirectBuffer}.
 * <p>
 * This class requires {@link UnsafeAccess} and Agrona must be in the classpath.
 */
public final class MutableDirectBufferProxy extends
    BufferProxy<MutableDirectBuffer> {

  /**
   * The {@link MutableDirectBuffer} proxy. Guaranteed to never be null,
   * although a class initialization exception will occur if an attempt is made
   * to access this field when unsafe or Agrona is unavailable.
   */
  public static final BufferProxy<MutableDirectBuffer> PROXY_MDB
      = new MutableDirectBufferProxy();

  @Override
  protected MutableDirectBuffer allocate(final int bytes) {
    ByteBuffer bb = allocateDirect(bytes);
    return new UnsafeBuffer(bb);
  }

  @Override
  protected void deallocate(final MutableDirectBuffer buff) {
  }

  @Override
  protected void in(final MutableDirectBuffer buffer, final Pointer ptr,
                    final long ptrAddr) {
    final long addr = buffer.addressOffset();
    final long size = buffer.capacity();
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr);
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
  }

  @Override
  protected void out(final MutableDirectBuffer buffer, final Pointer ptr,
                     final long ptrAddr) {
    final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
    final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
    buffer.wrap(addr, (int) size);
  }

}
