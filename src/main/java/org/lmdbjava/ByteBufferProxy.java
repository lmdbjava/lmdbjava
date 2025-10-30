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

import org.lmdbjava.Lmdb.MDB_val;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Comparator;

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.Env.SHOULD_CHECK;

/**
 * {@link ByteBuffer}-based proxy.
 */
public final class ByteBufferProxy extends BufferProxy<ByteBuffer> {

  static final ByteBufferProxy INSTANCE = new ByteBufferProxy();

  private ByteBufferProxy() {
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

  private static final Comparator<ByteBuffer> signedComparator =
          (o1, o2) -> {
            requireNonNull(o1);
            requireNonNull(o2);

            return o1.compareTo(o2);
          };
  private static final Comparator<ByteBuffer> unsignedComparator =
          new UnsignedByteBufferComparator();

  /**
   * A thread-safe pool for a given length. If the buffer found is valid (ie not of a negative
   * length) then that buffer is used. If no valid buffer is found, a new buffer is created.
   */
  private static final ThreadLocal<ArrayDeque<ByteBuffer>> BUFFERS =
          withInitial(() -> new ArrayDeque<>(16));

  @Override
  protected ByteBuffer allocate() {
    final ArrayDeque<ByteBuffer> queue = BUFFERS.get();
    final ByteBuffer buffer = queue.poll();

    if (buffer != null && buffer.capacity() >= 0) {
      return buffer;
    } else {
      return allocateDirect(0);
    }
  }

  @Override
  protected Comparator<ByteBuffer> getSignedComparator() {
    return signedComparator;
  }

  @Override
  protected Comparator<ByteBuffer> getUnsignedComparator() {
    return unsignedComparator;
  }

  @Override
  protected void deallocate(final ByteBuffer buff) {
    buff.order(BIG_ENDIAN);
    final ArrayDeque<ByteBuffer> queue = BUFFERS.get();
    queue.offer(buff);
  }

  @Override
  protected byte[] getBytes(final ByteBuffer buffer) {
    final byte[] dest = new byte[buffer.limit()];
    buffer.get(dest);
    return dest;
  }

  @Override
  protected void in(final ByteBuffer buffer, final MDB_val ptr) {
    if (SHOULD_CHECK && !buffer.isDirect()) {
      throw new BufferMustBeDirectException();
    }
    ptr.mvSize(buffer.remaining());
    ptr.mvData(MemorySegment.ofBuffer(buffer));
  }

  @Override
  protected void in(final ByteBuffer buffer, final int size, final MDB_val ptr) {
    if (SHOULD_CHECK && !buffer.isDirect()) {
      throw new BufferMustBeDirectException();
    }
    ptr.mvSize(size);
    ptr.mvData(MemorySegment.ofBuffer(buffer));
  }

  @Override
  protected ByteBuffer out(final MDB_val ptr) {
    final long size = ptr.mvSize();
    return ptr.mvData().reinterpret(size).asByteBuffer();
  }
}
