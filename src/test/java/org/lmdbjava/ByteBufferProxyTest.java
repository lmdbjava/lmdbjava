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

import static java.lang.Integer.BYTES;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import static org.lmdbjava.ByteBufferProxy.AbstractByteBufferProxy.findField;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.PROXY_SAFE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;
import static org.lmdbjava.UnsafeAccess.ALLOW_UNSAFE;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import org.junit.jupiter.api.Test;
import org.lmdbjava.ByteBufferProxy.BufferMustBeDirectException;
import org.lmdbjava.Env.ReadersFullException;

/** Test {@link ByteBufferProxy}. */
public final class ByteBufferProxyTest {

  static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();

  @Test
  void buffersMustBeDirect() {
    assertThatThrownBy(
            () -> {
              FileUtil.useTempDir(
                  dir -> {
                    try (Env<ByteBuffer> env = create().setMaxReaders(1).open(dir.toFile())) {
                      final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
                      final ByteBuffer key = allocate(100);
                      key.putInt(1).flip();
                      final ByteBuffer val = allocate(100);
                      val.putInt(1).flip();
                      db.put(key, val); // error
                    }
                  });
            })
        .isInstanceOf(BufferMustBeDirectException.class);
  }

  @Test
  void byteOrderResets() {
    final int retries = 100;
    for (int i = 0; i < retries; i++) {
      final ByteBuffer bb = PROXY_OPTIMAL.allocate();
      bb.order(LITTLE_ENDIAN);
      PROXY_OPTIMAL.deallocate(bb);
    }
    for (int i = 0; i < retries; i++) {
      assertThat(PROXY_OPTIMAL.allocate().order()).isEqualTo(BIG_ENDIAN);
    }
  }

  @Test
  void coverPrivateConstructor() {
    invokePrivateConstructor(ByteBufferProxy.class);
  }

  @Test
  void fieldNeverFound() {
    assertThatThrownBy(
            () -> {
              findField(Exception.class, "notARealField");
            })
        .isInstanceOf(LmdbException.class);
  }

  @Test
  void fieldSuperclassScan() {
    final Field f = findField(ReadersFullException.class, "rc");
    assertThat(f).isNotNull();
  }

  @Test
  void inOutBuffersProxyOptimal() {
    checkInOut(PROXY_OPTIMAL);
  }

  @Test
  void inOutBuffersProxySafe() {
    checkInOut(PROXY_SAFE);
  }

  @Test
  void optimalAlwaysAvailable() {
    final BufferProxy<ByteBuffer> v = PROXY_OPTIMAL;
    assertThat(v).isNotNull();
  }

  @Test
  void safeCanBeForced() {
    final BufferProxy<ByteBuffer> v = PROXY_SAFE;
    assertThat(v).isNotNull();
    assertThat(v.getClass().getSimpleName()).startsWith("Reflect");
  }

  @Test
  void unsafeIsDefault() {
    assertThat(ALLOW_UNSAFE).isTrue();
    final BufferProxy<ByteBuffer> v = PROXY_OPTIMAL;
    assertThat(v).isNotNull();
    assertThat(v).isNotEqualTo(PROXY_SAFE);
    assertThat(v.getClass().getSimpleName()).startsWith("Unsafe");
  }

  private void checkInOut(final BufferProxy<ByteBuffer> v) {
    // allocate a buffer larger than max key size
    final ByteBuffer b = allocateDirect(1_000);
    b.putInt(1);
    b.putInt(2);
    b.putInt(3);
    b.flip();
    b.position(BYTES); // skip 1

    final Pointer p = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    v.in(b, p);

    final ByteBuffer bb = allocateDirect(1);
    v.out(bb, p);

    assertThat(bb.getInt()).isEqualTo(2);
    assertThat(bb.getInt()).isEqualTo(3);
    assertThat(bb.remaining()).isEqualTo(0);
  }
}
