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
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lmdbjava.ByteBufferProxy.BufferMustBeDirectException;
import org.lmdbjava.Env.ReadersFullException;

/**
 * Test {@link ByteBufferProxy}.
 */
public final class ByteBufferProxyTest {

  static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();

  @Test
  void buffersMustBeDirect() {
    assertThatThrownBy(
        () -> {
          FileUtil.useTempDir(
              dir -> {
                try (Env<ByteBuffer> env = create()
                    .setMaxReaders(1)
                    .open(dir)) {
                  final Dbi<ByteBuffer> db = env.createDbi()
                      .setDbName(DB_1)
                      .withDefaultComparator()
                      .setDbiFlags(MDB_CREATE)
                      .open();
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

  @Test
  public void comparatorPerformance() {
    final Random random = new Random(345098);
    final ByteBuffer buffer1 = ByteBuffer.allocateDirect(Long.BYTES);
    final ByteBuffer buffer2 = ByteBuffer.allocateDirect(Long.BYTES);
    buffer1.limit(Long.BYTES);
    buffer2.limit(Long.BYTES);
    final long[] values = random.longs(10_000_000).toArray();
    final int rounds = 100;

    for (int run = 0; run < 3; run++) {
      Instant time = Instant.now();
      // x is to ensure result is used by the jvm
      int x = 0;
      for (int round = 0; round < rounds; round++) {
        for (int i = 1; i < values.length; i++) {
          buffer1.order(ByteOrder.nativeOrder())
              .putLong(0, values[i - 1]);
          buffer2.order(ByteOrder.nativeOrder())
              .putLong(0, values[i]);
          final int result = ByteBufferProxy.AbstractByteBufferProxy.compareAsIntegerKeys(buffer1, buffer2);
          x += result;
        }
      }
      System.out.println("compareAsIntegerKeys: " + Duration.between(time, Instant.now()) + ", x: " + x);

      time = Instant.now();
      int y = 0;
      for (int round = 0; round < rounds; round++) {
        for (int i = 1; i < values.length; i++) {
          buffer1.order(BIG_ENDIAN)
              .putLong(0, values[i - 1]);
          buffer2.order(BIG_ENDIAN)
              .putLong(0, values[i]);
          final int result = ByteBufferProxy.AbstractByteBufferProxy.compareLexicographically(buffer1, buffer2);
          y += result;
        }
      }
      System.out.println("compareLexicographically: " + Duration.between(time, Instant.now()) + ", y: " + y);

      assertThat(y).isEqualTo(x);
    }
  }

  @Test
  public void verifyComparators() {
    final Random random = new Random(203948);
    final ByteBuffer buffer1native = ByteBuffer.allocateDirect(Long.BYTES).order(ByteOrder.nativeOrder());
    final ByteBuffer buffer2native = ByteBuffer.allocateDirect(Long.BYTES).order(ByteOrder.nativeOrder());
    final ByteBuffer buffer1be = ByteBuffer.allocateDirect(Long.BYTES).order(BIG_ENDIAN);
    final ByteBuffer buffer2be = ByteBuffer.allocateDirect(Long.BYTES).order(BIG_ENDIAN);
    buffer1native.limit(Long.BYTES);
    buffer2native.limit(Long.BYTES);
    buffer1be.limit(Long.BYTES);
    buffer2be.limit(Long.BYTES);
    final long[] values = random.longs()
        .filter(i -> i >= 0)
        .limit(5_000_000)
        .toArray();
//    System.out.println("stats: " + Arrays.stream(values)
//        .summaryStatistics()
//        .toString());

    final LinkedHashMap<String, Comparator<ByteBuffer>> comparators = new LinkedHashMap<>();
    comparators.put("compareAsIntegerKeys", ByteBufferProxy.AbstractByteBufferProxy::compareAsIntegerKeys);
    comparators.put("compareLexicographically", ByteBufferProxy.AbstractByteBufferProxy::compareLexicographically);

    final LinkedHashMap<String, Integer> results = new LinkedHashMap<>(comparators.size());
    final Set<Integer> uniqueResults = new HashSet<>(comparators.size());

    for (int i = 1; i < values.length; i++) {
      final long val1 = values[i - 1];
      final long val2 = values[i];
      buffer1native.putLong(0, val1);
      buffer2native.putLong(0, val2);
      buffer1be.putLong(0, val1);
      buffer2be.putLong(0, val2);
      uniqueResults.clear();

      // Make sure all comparators give the same result for the same inputs
      comparators.forEach((name, comparator) -> {
        final int result;
        // IntegerKey comparator expects keys to have been written in native order so need different buffers.
        if (name.equals("compareAsIntegerKeys")) {
          result = comparator.compare(buffer1native, buffer2native);
        } else {
          result = comparator.compare(buffer1be, buffer2be);
        }
        results.put(name, result);
        uniqueResults.add(result);
      });

      if (uniqueResults.size() != 1) {
        Assertions.fail("Comparator mismatch for values: "
            + val1 + " and "
            + val2 + ". Results: " + results);
      }
    }
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
