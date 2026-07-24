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
import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers heap (address-less) buffers through {@link ByteBufProxy#PROXY_NETTY}. Previously any
 * buffer with {@code hasMemoryAddress() == false} made {@code in()} throw {@code
 * UnsupportedOperationException} from {@code ByteBuf.memoryAddress()}. This is the
 * version-independent root of lmdbjava#261 (Netty 4.2's adaptive allocator can hand back heap
 * buffers); it reproduces on 4.1 with any heap buffer.
 */
final class ByteBufHeapBufferTest {

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

  /** A heap value must round-trip (key direct, value heap). */
  @Test
  void putGet_withHeapValue() {
    try (Env<ByteBuf> env = openEnv()) {
      final Dbi<ByteBuf> db =
          env.createDbi().setDbName("db").withDefaultComparator().addDbiFlag(MDB_CREATE).open();
      final ByteBuf key = PooledByteBufAllocator.DEFAULT.directBuffer(env.getMaxKeySize());
      final ByteBuf value = PooledByteBufAllocator.DEFAULT.heapBuffer(64);
      try {
        assertThat(value.hasMemoryAddress()).isFalse(); // sanity: exercising the heap path
        key.writeCharSequence("greeting", UTF_8);
        value.writeCharSequence(VALUE, UTF_8);
        db.put(key, value);
        try (Txn<ByteBuf> txn = env.txnRead()) {
          final ByteBuf found = db.get(txn, key);
          assertThat(found).isNotNull();
          final byte[] got = new byte[found.readableBytes()];
          found.getBytes(found.readerIndex(), got);
          assertThat(got).isEqualTo(VALUE_BYTES);
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }

  /** A heap key must work for both put and lookup. */
  @Test
  void putGet_withHeapKeyAndValue() {
    try (Env<ByteBuf> env = openEnv()) {
      final Dbi<ByteBuf> db =
          env.createDbi().setDbName("db").withDefaultComparator().addDbiFlag(MDB_CREATE).open();
      final ByteBuf key = PooledByteBufAllocator.DEFAULT.heapBuffer(env.getMaxKeySize());
      final ByteBuf value = PooledByteBufAllocator.DEFAULT.heapBuffer(64);
      try {
        assertThat(key.hasMemoryAddress()).isFalse();
        key.writeCharSequence("greeting", UTF_8);
        value.writeCharSequence(VALUE, UTF_8);
        db.put(key, value);
        try (Txn<ByteBuf> txn = env.txnRead()) {
          final ByteBuf found = db.get(txn, key); // heap key lookup
          assertThat(found).isNotNull();
          final byte[] got = new byte[found.readableBytes()];
          found.getBytes(found.readerIndex(), got);
          assertThat(got).isEqualTo(VALUE_BYTES);
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }
}
