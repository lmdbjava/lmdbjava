/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
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
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import jnr.ffi.Pointer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import static org.lmdbjava.UnsafeAccess.UNSAFE;

/**
 * A buffer proxy backed by Agrona's {@link DirectBuffer}.
 *
 * <p>
 * This class requires {@link UnsafeAccess} and Agrona must be in the classpath.
 */
public final class DirectBufferProxy extends
    BufferProxy<DirectBuffer> {

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
  protected void deallocate(final DirectBuffer buff) {
    final OneToOneConcurrentArrayQueue<DirectBuffer> q = BUFFERS.get();
    q.offer(buff);
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
