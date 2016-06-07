package org.lmdbjava;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Static constants and methods that are convenient when writing LMDB-related
 * tests.
 */
public final class TestUtils {

  public static final String DB_1 = "test-db-1";
  public static final int POSIX_MODE = 0664;

  private TestUtils() {
  }

  static ByteBuffer createBb() {
    ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return bb;
  }

  static ByteBuffer createBb(int value) {
    ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(value).flip();
    return bb;
  }
}
