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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Byte array proxy.
 *
 * <p>{@link Env#create(org.lmdbjava.BufferProxy)}.
 */
public final class ByteArrayProxy extends BufferProxy<byte[]> {

  private final Arena arena;

  private static final Comparator<byte[]> signedComparator = ByteArrayProxy::compareArraysSigned;
  private static final Comparator<byte[]> unsignedComparator = ByteArrayProxy::compareArrays;

  public ByteArrayProxy(final Arena arena) {
    this.arena = arena;
  }

  /**
   * Lexicographically compare two byte arrays.
   *
   * @param o1 left operand (required)
   * @param o2 right operand (required)
   * @return as specified by {@link Comparable} interface
   */
  public static int compareArrays(final byte[] o1, final byte[] o2) {
    requireNonNull(o1);
    requireNonNull(o2);
    if (o1 == o2) {
      return 0;
    }
    final int minLength = min(o1.length, o2.length);

    for (int i = 0; i < minLength; i++) {
      final int lw = Byte.toUnsignedInt(o1[i]);
      final int rw = Byte.toUnsignedInt(o2[i]);
      final int result = Integer.compareUnsigned(lw, rw);
      if (result != 0) {
        return result;
      }
    }

    return o1.length - o2.length;
  }

  /**
   * Compare two byte arrays.
   *
   * @param b1 left operand (required)
   * @param b2 right operand (required)
   * @return as specified by {@link Comparable} interface
   */
  public static int compareArraysSigned(final byte[] b1, final byte[] b2) {
    requireNonNull(b1);
    requireNonNull(b2);

    if (b1 == b2) return 0;

    for (int i = 0; i < min(b1.length, b2.length); ++i) {
      if (b1[i] != b2[i]) return b1[i] - b2[i];
    }

    return b1.length - b2.length;
  }

  @Override
  protected byte[] allocate() {
    return new byte[0];
  }

  @Override
  protected void deallocate(final byte[] buff) {
    // byte arrays cannot be allocated
  }

  @Override
  protected byte[] getBytes(final byte[] buffer) {
    return Arrays.copyOf(buffer, buffer.length);
  }

  @Override
  protected Comparator<byte[]> getSignedComparator() {
    return signedComparator;
  }

  @Override
  protected Comparator<byte[]> getUnsignedComparator() {
    return unsignedComparator;
  }

  @Override
  protected void in(final byte[] buffer, final MDB_val ptr) {
    final MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, buffer);
    ptr.mvSize(buffer.length);
    ptr.mvData(segment);
  }

  @Override
  protected void in(final byte[] buffer, final int size, final MDB_val ptr) {
    // cannot reserve for byte arrays
  }

  @Override
  protected byte[] out(final MDB_val ptr) {
    final ByteBuffer byteBuffer = ptr.mvData().reinterpret(ptr.mvSize()).asByteBuffer();
    final byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(0, bytes, 0, byteBuffer.remaining());
    return bytes;
  }
}
