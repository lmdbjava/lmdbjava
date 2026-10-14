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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproduces and fixes lmdbjava#215: {@code ByteBuf.nioBuffer()} on a value returned via {@link
 * ByteBufProxy#PROXY_NETTY} yields zeros. Two complementary helpers are offered:
 *
 * <ul>
 *   <li>{@link ByteBufProxy#nioBufferView(ByteBuf)} — zero-copy, valid only within the txn.
 *   <li>{@link ByteBufProxy#nioBufferCopy(ByteBuf)} — an independent copy that outlives the txn,
 *       optionally into a caller-supplied buffer.
 * </ul>
 */
final class ByteBufNioBufferTest {

  private static final String VALUE = "Hello World";
  private static final byte[] VALUE_BYTES = VALUE.getBytes(UTF_8);

  private TempDir tempDir;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
  }

  @AfterEach
  void afterEach() {
    tempDir.cleanup();
  }

  private Env<ByteBuf> openEnv() {
    final Path dir = tempDir.createTempDir();
    return Env.create(ByteBufProxy.PROXY_NETTY).setMapSize(10_485_760).setMaxDbs(1).open(dir);
  }

  private static Dbi<ByteBuf> openDb(final Env<ByteBuf> env) {
    return env.createDbi().setDbName("db").withDefaultComparator().addDbiFlag(MDB_CREATE).open();
  }

  private static byte[] readable(final ByteBuf buffer) {
    final byte[] dst = new byte[buffer.readableBytes()];
    buffer.getBytes(buffer.readerIndex(), dst);
    return dst;
  }

  private static byte[] drain(final ByteBuffer buffer) {
    final byte[] dst = new byte[buffer.remaining()];
    buffer.get(dst);
    return dst;
  }

  /** Zero-copy view reflects the stored bytes (read inside the txn). */
  @Test
  void nioBufferView_reflectsStoredData() {
    try (Env<ByteBuf> env = openEnv()) {
      final Dbi<ByteBuf> db = openDb(env);
      final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
      final ByteBuf value = PooledByteBufAllocator.DEFAULT.directBuffer(64);
      try {
        key.writeCharSequence("greeting", UTF_8);
        value.writeCharSequence(VALUE, UTF_8);
        db.put(key, value);
        try (Txn<ByteBuf> txn = env.txnRead()) {
          final ByteBuf found = db.get(txn, key);
          assertThat(found).isNotNull();
          assertThat(readable(found)).isEqualTo(VALUE_BYTES); // sanity
          assertThat(drain(ByteBufProxy.nioBufferView(found))).isEqualTo(VALUE_BYTES);
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }

  /** Copy reflects the stored bytes. */
  @Test
  void nioBufferCopy_reflectsStoredData() {
    try (Env<ByteBuf> env = openEnv()) {
      final Dbi<ByteBuf> db = openDb(env);
      final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
      final ByteBuf value = PooledByteBufAllocator.DEFAULT.directBuffer(64);
      try {
        key.writeCharSequence("greeting", UTF_8);
        value.writeCharSequence(VALUE, UTF_8);
        db.put(key, value);
        try (Txn<ByteBuf> txn = env.txnRead()) {
          final ByteBuf found = db.get(txn, key);
          assertThat(found).isNotNull();
          assertThat(drain(ByteBufProxy.nioBufferCopy(found))).isEqualTo(VALUE_BYTES);
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }

  /** The discriminator: a copy taken inside the txn is still valid after txn AND env are closed. */
  @Test
  void nioBufferCopy_survivesTxnAndEnvClose() {
    final Env<ByteBuf> env = openEnv();
    final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
    final ByteBuf value = PooledByteBufAllocator.DEFAULT.directBuffer(64);
    ByteBuffer copy = null;
    try {
      final Dbi<ByteBuf> db = openDb(env);
      key.writeCharSequence("greeting", UTF_8);
      value.writeCharSequence(VALUE, UTF_8);
      db.put(key, value);
      try (Txn<ByteBuf> txn = env.txnRead()) {
        copy = ByteBufProxy.nioBufferCopy(db.get(txn, key));
      }
    } finally {
      key.release();
      value.release();
      env.close(); // both txn and env are now closed
    }
    assertThat(copy).isNotNull();
    assertThat(drain(copy)).isEqualTo(VALUE_BYTES); // copy is independent of LMDB memory
  }

  /**
   * The per-thread base buffer is reused across calls; an earlier view must still see its own value
   * after a later call on the same thread has repointed the base at another value.
   */
  @Test
  void nioBufferView_earlierViewUnaffectedByLaterCall() {
    final byte[] otherBytes = "Another value".getBytes(UTF_8);
    try (Env<ByteBuf> env = openEnv()) {
      final Dbi<ByteBuf> db = openDb(env);
      final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
      final ByteBuf value = PooledByteBufAllocator.DEFAULT.directBuffer(64);
      try {
        key.writeCharSequence("a", UTF_8);
        value.writeBytes(VALUE_BYTES);
        db.put(key, value);
        key.clear().writeCharSequence("b", UTF_8);
        value.clear().writeBytes(otherBytes);
        db.put(key, value);
        try (Txn<ByteBuf> txn = env.txnRead()) {
          key.clear().writeCharSequence("a", UTF_8);
          final ByteBuffer first = ByteBufProxy.nioBufferView(db.get(txn, key));
          key.clear().writeCharSequence("b", UTF_8);
          final ByteBuffer second = ByteBufProxy.nioBufferView(db.get(txn, key));
          assertThat(drain(second)).isEqualTo(otherBytes);
          assertThat(drain(first)).isEqualTo(VALUE_BYTES);
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }

  /**
   * The supplier overload copies into the caller's buffer: it is asked for exactly the readable
   * size, the same instance is returned flipped, and the copy outlives the txn and env.
   */
  @Test
  void nioBufferCopy_withSupplier_copiesIntoSuppliedBuffer() {
    final ByteBuffer pooled = ByteBuffer.allocateDirect(64);
    final AtomicInteger requested = new AtomicInteger(-1);
    final Env<ByteBuf> env = openEnv();
    final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
    final ByteBuf value = PooledByteBufAllocator.DEFAULT.directBuffer(64);
    ByteBuffer copy = null;
    try {
      final Dbi<ByteBuf> db = openDb(env);
      key.writeCharSequence("greeting", UTF_8);
      value.writeCharSequence(VALUE, UTF_8);
      db.put(key, value);
      try (Txn<ByteBuf> txn = env.txnRead()) {
        copy =
            ByteBufProxy.nioBufferCopy(
                db.get(txn, key),
                size -> {
                  requested.set(size);
                  return pooled;
                });
      }
    } finally {
      key.release();
      value.release();
      env.close();
    }
    assertThat(requested.get()).isEqualTo(VALUE_BYTES.length);
    assertThat(copy).isSameAs(pooled);
    assertThat(copy.position()).isZero();
    assertThat(copy.limit()).isEqualTo(VALUE_BYTES.length);
    assertThat(drain(copy)).isEqualTo(VALUE_BYTES);
  }

  /**
   * Bytes are written at the supplied buffer's position and nothing beyond the copied region is
   * touched, even when the supplied buffer is larger than needed.
   */
  @Test
  void nioBufferCopy_withSupplier_writesAtPositionAndLeavesRestUntouched() {
    final ByteBuf source = Unpooled.wrappedBuffer(VALUE_BYTES);
    final byte[] backing = new byte[32];
    Arrays.fill(backing, (byte) 0x7F);
    final ByteBuffer dest = ByteBuffer.wrap(backing);
    dest.position(3);

    final ByteBuffer result = ByteBufProxy.nioBufferCopy(source, size -> dest);

    assertThat(result).isSameAs(dest);
    assertThat(result.position()).isZero();
    assertThat(result.limit()).isEqualTo(3 + VALUE_BYTES.length);
    assertThat(Arrays.copyOfRange(backing, 3, 3 + VALUE_BYTES.length)).isEqualTo(VALUE_BYTES);
    assertThat(Arrays.copyOfRange(backing, 0, 3)).containsOnly((byte) 0x7F);
    assertThat(Arrays.copyOfRange(backing, 3 + VALUE_BYTES.length, backing.length))
        .containsOnly((byte) 0x7F);
    assertThat(source.readerIndex()).isZero(); // source indices are not consumed
  }

  @Test
  void nioBufferCopy_withSupplier_rejectsTooSmallBuffer() {
    final ByteBuf source = Unpooled.wrappedBuffer(VALUE_BYTES);
    assertThatThrownBy(
            () ->
                ByteBufProxy.nioBufferCopy(
                    source, size -> ByteBuffer.allocate(VALUE_BYTES.length - 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too small");
  }

  @Test
  void nioBufferCopy_withSupplier_rejectsNullBuffer() {
    final ByteBuf source = Unpooled.wrappedBuffer(VALUE_BYTES);
    assertThatThrownBy(() -> ByteBufProxy.nioBufferCopy(source, size -> null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Documents the lmdbjava#215 limitation: the raw {@link ByteBuf#nioBuffer()} does NOT reflect the
   * LMDB data (it views Netty's separate, never-repointed chunk buffer). This is why the two
   * helpers exist.
   */
  @Test
  void rawByteBufNioBuffer_doesNotReflectStoredData() {
    try (Env<ByteBuf> env = openEnv()) {
      final Dbi<ByteBuf> db = openDb(env);
      final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
      final ByteBuf value = PooledByteBufAllocator.DEFAULT.directBuffer(64);
      try {
        key.writeCharSequence("greeting", UTF_8);
        value.writeCharSequence(VALUE, UTF_8);
        db.put(key, value);
        try (Txn<ByteBuf> txn = env.txnRead()) {
          final ByteBuf found = db.get(txn, key);
          assertThat(found).isNotNull();
          assertThat(readable(found)).isEqualTo(VALUE_BYTES);
          assertThat(drain(found.nioBuffer())).isNotEqualTo(VALUE_BYTES);
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }
}
