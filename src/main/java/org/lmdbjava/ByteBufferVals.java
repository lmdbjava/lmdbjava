package org.lmdbjava;

import static java.lang.Boolean.getBoolean;
import static java.lang.Class.forName;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.ValB.STRUCT_FIELD_OFFSET_DATA;
import sun.misc.Unsafe;

/**
 * Provides the optimal {@link ByteBufferVal} for this JVM.
 */
public final class ByteBufferVals {

  /**
   * Java system property name that can be set to disable unsafe.
   */
  public static final String DISABLE_UNSAFE_PROP = "lmdbjava.disable.unsafe";

  /**
   * Indicates whether unsafe use is allowed.
   */
  public static final boolean ALLOW_UNSAFE = !getBoolean(DISABLE_UNSAFE_PROP);

  private static final Factory FACTORY;
  private static final String FIELD_NAME_CAPACITY = "capacity";
  private static final String FIELD_NAME_THE_UNSAFE = "theUnsafe";
  private static final String OUTER = ByteBufferVals.class.getName();
  static final String FIELD_NAME_ADDRESS = "address";
  static final String NAME_REFLECTIVE = OUTER + "$ReflectiveValFactory";
  static final String NAME_UNSAFE = OUTER + "$UnsafeValFactory";
  static final boolean SUPPORTS_UNSAFE;

  static {
    boolean supportsUnsafe = false;

    Factory unsafe = null;
    try {
      unsafe = (Factory) forName(NAME_UNSAFE).newInstance();
      supportsUnsafe = true;
    } catch (ClassNotFoundException | IllegalAccessException |
             InstantiationException ignore) {
    }
    SUPPORTS_UNSAFE = supportsUnsafe;

    Factory reflective = null;
    try {
      reflective = (Factory) forName(NAME_REFLECTIVE).newInstance();
    } catch (ClassNotFoundException | IllegalAccessException |
             InstantiationException ex) {
      throw new RuntimeException("Reflective factory failed", ex);
    }

    if (ALLOW_UNSAFE && unsafe != null) {
      FACTORY = unsafe;
    } else {
      FACTORY = reflective;
    }
  }

  /**
   * Create a new automatically refreshing {@link ByteBufferVal} for the passed
   * {@link ByteBuffer}.
   *
   * @param buffer instance to use
   * @return an initialized, automatically-refreshing instance (never null)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public static ByteBufferVal forBuffer(final ByteBuffer buffer) throws
      BufferNotDirectException {
    return FACTORY.forBuffer(buffer, true);
  }

  /**
   * Create a new {@link ByteBufferVal} for the passed {@link ByteBuffer}.
   *
   * @param buffer      instance to use
   * @param autoRefresh automatically refresh the buffer when updated by C
   * @return an initialized instance (never null)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public static ByteBufferVal forBuffer(final ByteBuffer buffer,
                                        final boolean autoRefresh) throws
      BufferNotDirectException {
    return FACTORY.forBuffer(buffer, autoRefresh);
  }

  /**
   * Create a new {@link ByteBufferVal} for the passed {@link ByteBuffer}.
   *
   * @param buffer      instance to use
   * @param autoRefresh automatically refresh the buffer when updated by C
   * @param forceSafe   forces use of safer JVM classes (not recommended)
   * @return an initialized instance (never null)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public static ByteBufferVal forBuffer(final ByteBuffer buffer,
                                        final boolean autoRefresh,
                                        final boolean forceSafe) throws
      BufferNotDirectException {
    if (forceSafe) {
      return new ReflectiveByteBufferVal(buffer, autoRefresh);
    }
    return FACTORY.forBuffer(buffer, autoRefresh);
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

  static void requireDirectBuffer(final Buffer buffer) throws
      BufferNotDirectException {
    if (!buffer.isDirect()) {
      throw new BufferNotDirectException();
    }

  }

  private ByteBufferVals() {
  }

  private static interface Factory {

    ByteBufferVal forBuffer(ByteBuffer buffer, final boolean autoRefresh) throws
        BufferNotDirectException;

  }

  static final class ReflectiveByteBufferVal extends ByteBufferVal {

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

    ReflectiveByteBufferVal(final ByteBuffer buffer, final boolean autoRefresh)
        throws BufferNotDirectException {
      super(buffer, autoRefresh);
    }

    @Override
    public long dataAddress() {
      return ptr.getLong(STRUCT_FIELD_OFFSET_DATA);
    }

    @Override
    public void refresh() {
      try {
        ADDRESS.set(bb, dataAddress());
        CAPACITY.set(bb, (int) size());
      } catch (IllegalArgumentException | IllegalAccessException ex) {
        throw new RuntimeException("Cannot modify buffer", ex);
      }
      bb.clear();
    }

    @Override
    public long size() {
      return ptr.getLong(STRUCT_FIELD_OFFSET_SIZE);
    }

    @Override
    void set() {
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, bb.capacity());
      ptr.putLong(STRUCT_FIELD_OFFSET_DATA, bbAddress);
    }

  }

  static final class ReflectiveValFactory implements Factory {

    ReflectiveValFactory() throws BufferNotDirectException {
      forBuffer(allocateDirect(0), false); // ensure success
    }

    @Override
    public ByteBufferVal forBuffer(final ByteBuffer buffer,
                                   final boolean autoRefresh)
        throws BufferNotDirectException {
      return new ReflectiveByteBufferVal(buffer, autoRefresh);
    }

  }

  static final class UnsafeByteBufferVal extends ByteBufferVal {

    static final long ADDRESS;
    static final long CAPACITY;
    static final Unsafe UNSAFE;

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

    UnsafeByteBufferVal(final ByteBuffer buffer, final boolean autoRefresh)
        throws BufferNotDirectException {
      super(buffer, autoRefresh);
    }

    @Override
    public long dataAddress() {
      return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_DATA);
    }

    @Override
    public void refresh() {
      UNSAFE.putLong(bb, ADDRESS, dataAddress());
      UNSAFE.putInt(bb, CAPACITY, (int) size());
      bb.clear();
    }

    @Override
    public long size() {
      return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE);
    }

    @Override
    void set() {
      UNSAFE.putLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE, bb.capacity());
      UNSAFE.putLong(ptrAddress + STRUCT_FIELD_OFFSET_DATA, bbAddress);
    }

  }

  static final class UnsafeValFactory implements Factory {

    UnsafeValFactory() throws BufferNotDirectException {
      forBuffer(allocateDirect(0), false); // ensure success
    }

    @Override
    public ByteBufferVal forBuffer(final ByteBuffer buffer,
                                   final boolean autoRefresh)
        throws BufferNotDirectException {
      return new UnsafeByteBufferVal(buffer, autoRefresh);
    }

  }

}
