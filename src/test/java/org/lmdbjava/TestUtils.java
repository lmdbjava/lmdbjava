package org.lmdbjava;

import static java.lang.Integer.BYTES;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import org.agrona.MutableDirectBuffer;
import static org.lmdbjava.ByteBufferVal.forBuffer;
import org.lmdbjava.LmdbException.BufferNotDirectException;

/**
 * Static constants and methods that are convenient when writing LMDB-related
 * tests.
 */
public final class TestUtils {

  public static final String DB_1 = "test-db-1";
  public static final int POSIX_MODE = 0664;

  static ByteBuffer allocateBb(CursorB<ByteBuffer> c, int value) {
    final ByteBuffer b = c.allocate(BYTES);
    b.putInt(value).flip();
    return b;
  }

  static MutableDirectBuffer allocateMdb(CursorB<MutableDirectBuffer> c,
                                         int value) {
    final MutableDirectBuffer b = c.allocate(BYTES);
    b.putInt(0, value);
    return b;
  }

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
