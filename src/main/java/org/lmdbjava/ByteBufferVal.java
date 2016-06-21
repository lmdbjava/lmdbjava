/*
 * Copyright 2016 The LmdbJava Open Source Project.
 *
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
 */
package org.lmdbjava;

import static java.lang.Boolean.getBoolean;
import static java.lang.Class.forName;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.Env.SHOULD_CHECK;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.Val.STRUCT_FIELD_OFFSET_DATA;
import sun.misc.Unsafe;

/**
 * {@link ByteBuffer} value.
 * <p>
 * Only "direct" Java byte buffers can be used with LmdbJava. This allows the
 * LMDB C API to directly read and write the underlying memory.
 * <p>
 * As described in the {@link Val} contract, the underlying buffer address and
 * capacity will routinely change as a result of LMBC C calls. While Java byte
 * buffers do not provide publicly-accessible address information or permit the
 * modification of their address or capacity, the provided
 * <code>ByteBufferVal</code> implementations satisfy this requirement by
 * directly getting or setting the private fields of a byte buffer instance.
 * <p>
 * Use {@link #forBuffer(java.nio.ByteBuffer)} (and associated overloaded method
 * signatures) to obtain a concrete <code>ByteBufferVal</code> implementation.
 * There are two concrete implementations provided:
 * <ul>
 * <li>A "fast" implementation via {@link UnsafeByteBufferVal}</li>
 * <li>A "safe" implementation via {@link ReflectiveByteBufferVal}</li>
 * </ul>
 * <p>
 * The "fast" implementation is automatically used where possible. However the
 * "safe" implementation will be used in any of these cases:
 * <ul>
 * <li>Unsafe is unavailable</li>
 * <li>The {@link #DISABLE_UNSAFE_PROP} system property is
 * <code>true</code></li>
 * <li>An invocation of
 * {@link #forBuffer(java.nio.ByteBuffer, boolean, boolean)} sets the
 * <code>forceSafe</code> argument to <code>true</code></li>
 * </ul>
 */
public abstract class ByteBufferVal extends Val {

  /**
   * Java system property name that can be set to disable unsafe.
   */
  public static final String DISABLE_UNSAFE_PROP = "lmdbjava.disable.unsafe";
  /**
   * Indicates whether unsafe use is allowed.
   */
  public static final boolean ALLOW_UNSAFE = !getBoolean(DISABLE_UNSAFE_PROP);

  private static final Factory FACTORY_OPTIMAL;
  private static final Factory FACTORY_SAFE;
  private static final String FIELD_NAME_ADDRESS = "address";
  private static final String FIELD_NAME_CAPACITY = "capacity";
  private static final String FIELD_NAME_THE_UNSAFE = "theUnsafe";
  private static final String OUTER = ByteBufferVal.class.getName();
  private static final String NAME_UNSAFE = OUTER + "$UnsafeValFactory";
  private static final String NAME_REFLECTIVE = OUTER + "$ReflectiveValFactory";

  static {
    FACTORY_SAFE = factory(NAME_REFLECTIVE);
    requireNonNull(FACTORY_SAFE, "Mandatory reflective factory unavailable");
    final Factory unsafe = ALLOW_UNSAFE ? factory(NAME_UNSAFE) : null;
    FACTORY_OPTIMAL = unsafe == null ? FACTORY_SAFE : unsafe;
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
    return FACTORY_OPTIMAL.forBuffer(buffer, true);
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
    return FACTORY_OPTIMAL.forBuffer(buffer, autoRefresh);
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
      return FACTORY_SAFE.forBuffer(buffer, autoRefresh);
    }
    return FACTORY_OPTIMAL.forBuffer(buffer, autoRefresh);
  }

  /**
   * Safely instantiates the factory, hiding any exceptions.
   *
   * @param name class to instantiate
   * @return the initialized factory, or null if there was an exception
   */
  static Factory factory(final String name) {
    try {
      return (Factory) forName(name).newInstance();
    } catch (ClassNotFoundException | IllegalAccessException |
             ClassCastException | InstantiationException ignore) {
    }
    return null;
  }

  static Field findField(final Class<?> c, final String name) {
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
    throw new RuntimeException(name + " not found");
  }

  static void requireDirectBuffer(final Buffer buffer) throws
      BufferNotDirectException {
    if (!buffer.isDirect()) {
      throw new BufferNotDirectException();
    }
  }

  private final boolean autoRefresh;

  /**
   * Tracks whether the current {@link #bbAddress} and capacity have been
   * written to the <code>MDB_val</code>.
   */
  private boolean mdbValSet;

  /**
   * The byte buffer currently wrapped by this instance.
   */
  protected ByteBuffer bb;

  /**
   * The memory address where the byte buffer data commenced, as at the time
   * {@link #wrap(java.nio.ByteBuffer)} was invoked.
   */
  protected long bbAddress;

  /**
   * Create a new instance that uses the passed buffer.
   *
   * @param buffer      instance to use
   * @param autoRefresh automatically refresh the buffer when {@link #dirty()}
   *                    is called
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  ByteBufferVal(final ByteBuffer buffer, final boolean autoRefresh)
      throws BufferNotDirectException {
    super();
    wrap(buffer);
    this.autoRefresh = autoRefresh;
  }

  /**
   * Returns the internal buffer currently wrapped by this instance.
   *
   * @return the buffer (never null)
   */
  public final ByteBuffer buffer() {
    return bb;
  }

  /**
   * Set the internal buffer to the passed instance.
   *
   * @param buffer instance to use (required; must be direct)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public final void wrap(final ByteBuffer buffer) throws
      BufferNotDirectException {
    if (SHOULD_CHECK) {
      requireDirectBuffer(buffer);
    }
    this.bb = buffer;
    this.bbAddress = ((sun.nio.ch.DirectBuffer) buffer).address();
    this.mdbValSet = false;
  }

  @Override
  final protected void dirty() {
    if (autoRefresh) {
      refresh();
    }
  }

  /**
   * Called when a subclass should set the <code>MDB_val</code>.
   */
  protected abstract void onSet();

  @Override
  final protected void set() {
    if (mdbValSet) {
      return;
    }
    mdbValSet = true;
    onSet();
  }

  /**
   * A {@link ByteBufferVal} that uses Java reflection to modify byte buffer
   * fields, and official JNR-FFF methods to manipulate its allocated pointers.
   */
  static final class ReflectiveByteBufferVal extends ByteBufferVal {

    private static final Field ADDRESS_FIELD;
    private static final Field CAPACITY_FIELD;

    static {
      ADDRESS_FIELD = findField(Buffer.class, FIELD_NAME_ADDRESS);
      CAPACITY_FIELD = findField(Buffer.class, FIELD_NAME_CAPACITY);
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
        ADDRESS_FIELD.set(bb, dataAddress());
        CAPACITY_FIELD.set(bb, (int) size());
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
    protected void onSet() {
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

  /**
   * A {@link ByteBufferVal} that uses Java's "unsafe" class to directly
   * manipulate byte buffer fields and JNR-FFF allocated memory pointers.
   */
  static final class UnsafeByteBufferVal extends ByteBufferVal {

    static final long ADDRESS_OFFSET;
    static final long CAPACITY_OFFSET;
    static final Unsafe UNSAFE;

    static {
      try {
        final Field field = Unsafe.class.getDeclaredField(FIELD_NAME_THE_UNSAFE);
        field.setAccessible(true);
        UNSAFE = (Unsafe) field.get(null);
        final Field address = findField(Buffer.class,
                                        FIELD_NAME_ADDRESS);
        final Field capacity = findField(Buffer.class,
                                         FIELD_NAME_CAPACITY);
        ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
        CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity);
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
      UNSAFE.putLong(bb, ADDRESS_OFFSET, dataAddress());
      UNSAFE.putInt(bb, CAPACITY_OFFSET, (int) size());
      bb.clear();
    }

    @Override
    public long size() {
      return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE);
    }

    @Override
    protected void onSet() {
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

  private interface Factory {

    ByteBufferVal forBuffer(ByteBuffer buffer, final boolean autoRefresh) throws
        BufferNotDirectException;

  }

}
