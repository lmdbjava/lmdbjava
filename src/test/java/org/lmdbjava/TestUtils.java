package org.lmdbjava;

import static java.lang.Integer.BYTES;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import org.agrona.MutableDirectBuffer;

/**
 * Static constants and methods that are convenient when writing LMDB-related
 * tests.
 */
public final class TestUtils {

  public static final String DB_1 = "test-db-1";
  public static final int POSIX_MODE = 0664;

  static ByteBuffer allocateBb(final Txn<ByteBuffer> txn, final int value) {
    final ByteBuffer b = txn.allocate(BYTES);
    b.putInt(value).flip();
    return b;
  }

  static MutableDirectBuffer allocateMdb(final Txn<MutableDirectBuffer> txn,
                                         final int value) {
    final MutableDirectBuffer b = txn.allocate(BYTES);
    b.putInt(0, value);
    return b;
  }

  static ByteBuffer createBb(int value) {
    final ByteBuffer bb = allocateDirect(BYTES);
    bb.putInt(value).flip();
    return bb;
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
