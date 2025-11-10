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
import static java.nio.ByteBuffer.allocateDirect;

import io.netty.buffer.ByteBuf;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Static constants and methods that are convenient when writing LMDB-related tests. */
final class TestUtils {

  public static final String DB_1 = "test-db-1";
  public static final String DB_2 = "test-db-2";
  public static final String DB_3 = "test-db-3";
  public static final String DB_4 = "test-db-2";

  private TestUtils() {}

  static byte[] ba(final int value) {
    byte[] bytes = new byte[4];
    ByteBuffer.wrap(bytes).putInt(value);
    return bytes;
  }

  static int fromBa(final byte[] ba) {
    return ByteBuffer.wrap(ba).getInt();
  }

  static ByteBuffer bb(final int value) {
    final ByteBuffer bb = allocateDirect(Integer.BYTES);
    bb.putInt(value).flip();
    return bb;
  }

  static ByteBuffer bb(final long value) {
    final ByteBuffer bb = allocateDirect(Long.BYTES);
    bb.putLong(value).flip();
    return bb;
  }

  static ByteBuffer bb(final String value) {
    final ByteBuffer bb = allocateDirect(100);
    if (value != null) {
      bb.put(value.getBytes(StandardCharsets.UTF_8));
      bb.flip();
    }
    return bb;
  }

  static ByteBuffer bbNative(final int value) {
    final ByteBuffer bb = allocateDirect(Integer.BYTES).order(ByteOrder.nativeOrder());
    bb.putInt(value).flip();
    return bb;
  }

  static ByteBuffer bbNative(final long value) {
    final ByteBuffer bb = allocateDirect(Long.BYTES).order(ByteOrder.nativeOrder());
    bb.putLong(value).flip();
    return bb;
  }

  static int getNativeInt(final ByteBuffer bb) {
    final int val = bb.order(ByteOrder.nativeOrder()).getInt();
    bb.rewind();
    return val;
  }

  static long getNativeLong(final ByteBuffer bb) {
    final long val = bb.order(ByteOrder.nativeOrder()).getLong();
    bb.rewind();
    return val;
  }

  static long getNativeIntOrLong(final ByteBuffer bb) {
    if (bb.remaining() == Integer.BYTES) {
      return getNativeInt(bb);
    } else {
      return getNativeLong(bb);
    }
  }

  static String getString(final ByteBuffer bb) {
    final String str = StandardCharsets.UTF_8.decode(bb).toString();
    bb.rewind();
    return str;
  }

  static byte[] getBytes(final ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    final byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(bytes);
    return bytes;
  }

  static void invokePrivateConstructor(final Class<?> clazz) {
    try {
      final Constructor<?> c = clazz.getDeclaredConstructor();
      c.setAccessible(true);
      c.newInstance();
    } catch (final NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException e) {
      throw new LmdbException("Private construction failed", e);
    }
  }

  static MutableDirectBuffer mdb(final int value) {
    final MutableDirectBuffer b = new UnsafeBuffer(allocateDirect(Integer.BYTES));
    b.putInt(0, value);
    return b;
  }

  static ByteBuf nb(final int value) {
    final ByteBuf b = DEFAULT.directBuffer(Integer.BYTES);
    b.writeInt(value);
    return b;
  }

  static <T> void doWithReadTxn(final Env<T> env, final Consumer<Txn<T>> work) {
    Objects.requireNonNull(env);
    Objects.requireNonNull(work);
    try (Txn<T> readTxn = env.txnRead()) {
      work.accept(readTxn);
    }
  }

  static <T, R> R getWithReadTxn(final Env<T> env, final Function<Txn<T>, R> work) {
    Objects.requireNonNull(env);
    Objects.requireNonNull(work);
    try (Txn<T> readTxn = env.txnRead()) {
      return work.apply(readTxn);
    }
  }

  static <T> void doWithWriteTxn(final Env<T> env, final Consumer<Txn<T>> work) {
    Objects.requireNonNull(env);
    Objects.requireNonNull(work);
    try (Txn<T> readTxn = env.txnWrite()) {
      work.accept(readTxn);
    }
  }

  static <T, R> R getWithWriteTxn(final Env<T> env, final Function<Txn<T>, R> work) {
    Objects.requireNonNull(env);
    Objects.requireNonNull(work);
    try (Txn<T> readTxn = env.txnWrite()) {
      return work.apply(readTxn);
    }
  }
}
