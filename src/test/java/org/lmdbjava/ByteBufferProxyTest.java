/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2019 The LmdbJava Open Source Project
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

import java.io.File;
import java.io.IOException;
import static java.lang.Integer.BYTES;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import static org.lmdbjava.ByteBufferProxy.AbstractByteBufferProxy.findField;
import org.lmdbjava.ByteBufferProxy.BufferMustBeDirectException;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.PROXY_SAFE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env.ReadersFullException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Library.RUNTIME;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;
import static org.lmdbjava.UnsafeAccess.ALLOW_UNSAFE;

/**
 * Test {@link ByteBufferProxy}.
 */
public final class ByteBufferProxyTest {

  static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected = BufferMustBeDirectException.class)
  public void buffersMustBeDirect() throws IOException {
    final File path = tmp.newFolder();
    try (Env<ByteBuffer> env = create()
        .setMaxReaders(1)
        .open(path)) {
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
    final int retries = 100;
    for (int i = 0; i < retries; i++) {
      final ByteBuffer bb = PROXY_OPTIMAL.allocate();
      bb.order(LITTLE_ENDIAN);
      PROXY_OPTIMAL.deallocate(bb);
    }
    for (int i = 0; i < retries; i++) {
      assertThat(PROXY_OPTIMAL.allocate().order(), is(BIG_ENDIAN));
    }
  }

  @Test
  public void coverPrivateConstructor() {
    invokePrivateConstructor(ByteBufferProxy.class);
  }

  @Test(expected = LmdbException.class)
  public void fieldNeverFound() {
    findField(Exception.class, "notARealField");
  }

  @Test
  public void fieldSuperclassScan() {
    final Field f = findField(ReadersFullException.class, "rc");
    assertThat(f, is(notNullValue()));
  }

  @Test
  public void inOutBuffersProxyOptimal() {
    checkInOut(PROXY_OPTIMAL);
  }

  @Test
  public void inOutBuffersProxySafe() {
    checkInOut(PROXY_SAFE);
  }

  @Test
  public void optimalAlwaysAvailable() {
    final BufferProxy<ByteBuffer> v = PROXY_OPTIMAL;
    assertThat(v, is(notNullValue()));
  }

  @Test
  public void safeCanBeForced() {
    final BufferProxy<ByteBuffer> v = PROXY_SAFE;
    assertThat(v, is(notNullValue()));
    assertThat(v.getClass().getSimpleName(), startsWith("Reflect"));
  }

  @Test
  public void unsafeIsDefault() {
    assertThat(ALLOW_UNSAFE, is(true));
    final BufferProxy<ByteBuffer> v = PROXY_OPTIMAL;
    assertThat(v, is(notNullValue()));
    assertThat(v, is(not(PROXY_SAFE)));
    assertThat(v.getClass().getSimpleName(), startsWith("Unsafe"));
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
    v.in(b, p, p.address());

    final ByteBuffer bb = allocateDirect(1);
    v.out(bb, p, p.address());

    assertThat(bb.getInt(), is(2));
    assertThat(bb.getInt(), is(3));
    assertThat(bb.remaining(), is(0));
  }

}
