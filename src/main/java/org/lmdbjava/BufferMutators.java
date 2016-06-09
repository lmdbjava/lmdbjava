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

import static java.lang.Class.forName;
import java.lang.reflect.Field;
import java.nio.Buffer;
import sun.misc.Unsafe;

/**
 * Provides the optimal {@link BufferMutator} for this JVM.
 */
final class BufferMutators {

  private static final String FIELD_NAME_CAPACITY = "capacity";
  private static final String FIELD_NAME_THE_UNSAFE = "theUnsafe";
  private static final String OUTER = BufferMutators.class.getName();
  static final String FIELD_NAME_ADDRESS = "address";
  static final BufferMutator MUTATOR;
  static final String NAME_REFLECTIVE = OUTER + "$ReflectiveBufferMutator";
  static final String NAME_UNSAFE = OUTER + "$UnsafeBufferMutator";
  static final boolean SUPPORTS_UNSAFE;

  /**
   * Attempts to obtain an unsafe-based mutator, and if that fails, attempts to
   * obtain a reflection-based mutator.
   */
  static {
    BufferMutator mutator;
    boolean supportsUnsafe = false;
    try {
      mutator = load(NAME_UNSAFE);
      supportsUnsafe = true;
    } catch (ClassNotFoundException | IllegalAccessException |
             InstantiationException ignore) {
      try {
        mutator = load(NAME_REFLECTIVE);
      } catch (ClassNotFoundException | IllegalAccessException |
               InstantiationException ex) {
        throw new RuntimeException(NAME_REFLECTIVE + " unavailable", ex);
      }
    }
    MUTATOR = mutator;
    SUPPORTS_UNSAFE = supportsUnsafe;
  }

  private static BufferMutator load(final String className) throws
      ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (BufferMutator) forName(className).newInstance();
  }

  static Field findField(final Class<?> c, final String name) throws
      NoSuchFieldException {
    Class<?> clazz = c;

    do {
      try {
        final Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    } while (clazz != null);

    throw new NoSuchFieldException(name + " not found");
  }

  private BufferMutators() {
  }

  private static final class ReflectiveBufferMutator implements BufferMutator {

    private static final Field ADDRESS;
    private static final Field CAPACITY;

    static {
      try {
        ADDRESS = findField(Buffer.class, FIELD_NAME_ADDRESS);
        CAPACITY = findField(Buffer.class, FIELD_NAME_CAPACITY);
      } catch (NoSuchFieldException ex) {
        throw new RuntimeException(ex);
      }
    }

    ReflectiveBufferMutator() {
    }

    @Override
    public void modify(final Buffer buffer, final long address,
                       final int capacity) {
      try {
        ADDRESS.set(buffer, address);
        CAPACITY.set(buffer, capacity);
      } catch (IllegalArgumentException | IllegalAccessException ex) {
        throw new RuntimeException("Cannot modify buffer", ex);
      }
      buffer.clear();
    }
  }

  private static final class UnsafeBufferMutator implements BufferMutator {

    private static final long ADDRESS;
    private static final long CAPACITY;
    private static final Unsafe UNSAFE;

    static {
      try {
        final Field field = Unsafe.class.getDeclaredField(FIELD_NAME_THE_UNSAFE);
        field.setAccessible(true);
        UNSAFE = (Unsafe) field.get(null);
        final Field address = findField(Buffer.class, FIELD_NAME_ADDRESS);
        final Field capacity = findField(Buffer.class, FIELD_NAME_CAPACITY);
        ADDRESS = UNSAFE.objectFieldOffset(address);
        CAPACITY = UNSAFE.objectFieldOffset(capacity);
      } catch (NoSuchFieldException | SecurityException |
               IllegalArgumentException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    UnsafeBufferMutator() {
    }

    @Override
    public void modify(final Buffer buffer, final long address,
                       final int capacity) {
      UNSAFE.putLong(buffer, ADDRESS, address);
      UNSAFE.putInt(buffer, CAPACITY, capacity);
      buffer.clear();
    }
  }

}
