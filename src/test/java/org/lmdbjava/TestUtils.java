/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static java.lang.Integer.BYTES;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.lmdbjava.ByteBufferVals.forBuffer;
import org.lmdbjava.LmdbException.BufferNotDirectException;

/**
 * Static constants and methods that are convenient when writing LMDB-related
 * tests.
 */
public final class TestUtils {

  public static final String DB_1 = "test-db-1";
  public static final int POSIX_MODE = 0664;

  static ByteBuffer createBb() {
    ByteBuffer bb = allocateDirect(BYTES);
    bb.order(LITTLE_ENDIAN);
    return bb;
  }

  static ByteBuffer createBb(int value) {
    final ByteBuffer bb = allocateDirect(BYTES);
    bb.order(LITTLE_ENDIAN);
    bb.putInt(value).flip();
    return bb;
  }

  static ByteBufferVal createValB() throws BufferNotDirectException {
    return forBuffer(createBb());
  }

  static ByteBufferVal createValBb(int value) throws BufferNotDirectException {
    final ByteBuffer bb = allocateDirect(BYTES);
    bb.order(LITTLE_ENDIAN);
    bb.putInt(value).flip();
    return forBuffer(bb);
  }

  static void invokePrivateConstructor(final Class<?> clazz) throws
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException {
    final Constructor<?> c = clazz.getDeclaredConstructor();
    c.setAccessible(true);
    c.newInstance();
  }

  private TestUtils() {
  }
}
