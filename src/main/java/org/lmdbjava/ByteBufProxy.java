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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.lmdbjava.Lmdb.MDB_val;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Comparator;

import static io.netty.buffer.PooledByteBufAllocator.DEFAULT;
import static java.util.Objects.requireNonNull;

/**
 * A buffer proxy backed by Netty's {@link ByteBuf}.
 *
 * <p>This class requires netty-buffer in the classpath.
 */
public final class ByteBufProxy extends BufferProxy<ByteBuf> {

  /**
   * A proxy for using Netty {@link ByteBuf}. Guaranteed to never be null, although a class
   * initialization exception will occur if an attempt is made to access this field when Netty is
   * unavailable.
   */
  public static final BufferProxy<ByteBuf> PROXY_NETTY = new ByteBufProxy();

  private static final int BUFFER_RETRIES = 10;
  private static final String NAME = "io.netty.buffer.PooledUnsafeDirectByteBuf";
  private static final Comparator<ByteBuf> comparator =
          (o1, o2) -> {
            requireNonNull(o1);
            requireNonNull(o2);

            return o1.compareTo(o2);
          };

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
  protected Comparator<ByteBuf> getSignedComparator() {
    return comparator;
  }

  @Override
  protected Comparator<ByteBuf> getUnsignedComparator() {
    return comparator;
  }

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
  protected void in(final ByteBuf buffer, final MDB_val ptr) {
    ptr.mvSize(buffer.writerIndex() - buffer.readerIndex());
    ptr.mvData(MemorySegment.ofBuffer(buffer.nioBuffer()));
  }

  @Override
  protected void in(final ByteBuf buffer, final int size, final MDB_val ptr) {
    ptr.mvSize(size);
    ptr.mvData(MemorySegment.ofBuffer(buffer.nioBuffer()));
  }

  @Override
  protected ByteBuf out(final MDB_val ptr) {
    final long size = ptr.mvSize();
    final ByteBuffer byteBuffer = ptr.mvData().reinterpret(size).asByteBuffer();
    return Unpooled.wrappedBuffer(byteBuffer);
  }
}
