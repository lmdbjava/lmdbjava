/*
 * Copyright LWJGL. All rights reserved.
 * License terms: http://lwjgl.org/license.php
 */
package org.lmdbjava.core.lli;

import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import org.lmdbjava.core.lli.Library.MDB_val;

import java.nio.*;

import static org.lmdbjava.core.lli.Library.runtime;

abstract class MemoryAccess {

  public static final sun.misc.Unsafe UNSAFE;
  private static final long ADDRESS;
  private static final long CAPACITY;

  static {
    try {
      UNSAFE = getUnsafeInstance();
      ADDRESS = UNSAFE.objectFieldOffset(getDeclaredField(Buffer.class,
                                                          "address"));
      CAPACITY = UNSAFE.objectFieldOffset(getDeclaredField(Buffer.class,
                                                           "capacity"));
    } catch (Exception e) {
      throw new UnsupportedOperationException(e);
    }
  }

  static void wrap(final ByteBuffer buffer, MDB_val val) {
    UNSAFE.putLong(buffer, ADDRESS, val.data.get().address());
    UNSAFE.putInt(buffer, CAPACITY, (int) val.size.get());
    buffer.clear();
  }

  static MDB_val createVal(ByteBuffer bb) {
    if (!bb.isDirect()) {
      throw new IllegalArgumentException("ByteBuffer is not direct");
    }
    MDB_val val = new MDB_val(runtime);
    val.size.set(bb.limit());
    val.data.set(new ByteBufferMemoryIO(runtime, bb));
    return val;
  }


  private static java.lang.reflect.Field getDeclaredField(Class<?> root,
                                                          String fieldName)
      throws NoSuchFieldException {
    Class<?> type = root;

    do {
      try {
        java.lang.reflect.Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      }
    } while (type != null);

    throw new NoSuchFieldException(fieldName + " does not exist in " + root.
        getSimpleName() + " or any of its superclasses.");
  }

  private static sun.misc.Unsafe getUnsafeInstance() {
    java.lang.reflect.Field[] fields = sun.misc.Unsafe.class.
        getDeclaredFields();

    /*
     * Different runtimes use different names for the Unsafe singleton, so we
     * cannot use .getDeclaredField and we scan instead. For example:
     *
     * Oracle: theUnsafe PERC : m_unsafe_instance Android: THE_ONE
     */
    for (java.lang.reflect.Field field : fields) {
      if (!field.getType().equals(sun.misc.Unsafe.class)) {
        continue;
      }

      int modifiers = field.getModifiers();
      if (!(java.lang.reflect.Modifier.isStatic(modifiers)
            && java.lang.reflect.Modifier.isFinal(modifiers))) {
        continue;
      }

      field.setAccessible(true);
      try {
        return (sun.misc.Unsafe) field.get(null);
      } catch (IllegalAccessException e) {
        // ignore
      }
      break;
    }

    throw new UnsupportedOperationException();
  }

}
