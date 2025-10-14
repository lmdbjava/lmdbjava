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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.ByteBufferProxy.BufferMustBeDirectException;
import org.lmdbjava.Lmdb.MDB_val;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;

import static java.lang.Integer.BYTES;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

/**
 * Test {@link ByteBufferProxy}.
 */
public final class ByteBufferProxyTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected = BufferMustBeDirectException.class)
  public void buffersMustBeDirect() throws IOException {
    final File path = tmp.newFolder();
    try (Env<ByteBuffer> env = create().setMaxReaders(1).open(path)) {
      final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
      final ByteBuffer key = allocate(100);
      key.putInt(1).flip();
      final ByteBuffer val = allocate(100);
      val.putInt(1).flip();
      db.put(key, val); // error
    }
  }

  @Test
  public void byteOrderResets() {
    final ByteBufferProxy byteBufferProxy = ByteBufferProxy.INSTANCE;
    final int retries = 100;
    for (int i = 0; i < retries; i++) {
      final ByteBuffer bb = byteBufferProxy.allocate();
      bb.order(LITTLE_ENDIAN);
      byteBufferProxy.deallocate(bb);
    }
    for (int i = 0; i < retries; i++) {
      assertThat(byteBufferProxy.allocate().order(), is(BIG_ENDIAN));
    }
  }

  @Test
  public void coverPrivateConstructor() {
    invokePrivateConstructor(ByteBufferProxy.class);
  }

  @Test
  public void inOutBuffersProxy() {
    checkInOut(ByteBufferProxy.INSTANCE);
  }

  private void checkInOut(final BufferProxy<ByteBuffer> v) {
    // allocate a buffer larger than max key size
    final ByteBuffer b = allocateDirect(1_000);
    b.putInt(1);
    b.putInt(2);
    b.putInt(3);
    b.flip();
    b.position(BYTES); // skip 1

    try (final Arena arena = Arena.ofConfined()) {
      final MDB_val p = new MDB_val(arena);
      v.in(b, p);

      final ByteBuffer bb = v.out(p);

      assertThat(bb.getInt(), is(2));
      assertThat(bb.getInt(), is(3));
      assertThat(bb.remaining(), is(0));
    }
  }
}
