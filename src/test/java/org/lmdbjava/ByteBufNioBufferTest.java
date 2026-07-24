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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproduces lmdbjava#215: {@code ByteBuf.nioBuffer()} on a value returned via {@link
 * ByteBufProxy#PROXY_NETTY} does not reflect the stored data (it views Netty's separate,
 * chunk-shared backing buffer, which the zero-copy read path never repoints).
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

  /**
   * Documents the lmdbjava#215 limitation: the raw {@link ByteBuf#nioBuffer()} does NOT reflect the
   * LMDB data, even though the {@link ByteBuf}'s own accessors do.
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
          assertThat(readable(found)).isEqualTo(VALUE_BYTES); // ByteBuf accessors are correct
          assertThat(drain(found.nioBuffer())).isNotEqualTo(VALUE_BYTES); // nioBuffer() is not
        }
      } finally {
        key.release();
        value.release();
      }
    }
  }
}
