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

import static java.util.Objects.requireNonNull;
import org.agrona.MutableDirectBuffer;
import static org.lmdbjava.ByteBufferVal.requireDirectBuffer;
import static org.lmdbjava.ByteBufferVals.UnsafeByteBufferVal.UNSAFE;
import static org.lmdbjava.Env.SHOULD_CHECK;
import org.lmdbjava.LmdbException.BufferNotDirectException;

/**
 * Agrona buffer value.
 * <p>
 * This class requires unsafe to operate.
 */
public final class MutableDirectBufferVal extends Val {

  /**
   * Create a new automatically refreshing {@link MutableDirectBufferVal} for
   * the passed {@link MutableDirectBuffer}.
   *
   * @param buffer instance to use
   * @return an initialized, automatically-refreshing instance (never null)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public static MutableDirectBufferVal forMdb(
      final MutableDirectBuffer buffer) throws
      BufferNotDirectException {
    return new MutableDirectBufferVal(buffer, true);
  }

  /**
   * Create a new {@link MutableDirectBufferVal} for the passed
   * {@link MutableDirectBuffer}.
   *
   * @param buffer      instance to use
   * @param autoRefresh automatically refresh the buffer when updated by C
   * @return an initialized instance (never null)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public static MutableDirectBufferVal forMdb(
      final MutableDirectBuffer buffer,
      final boolean autoRefresh)
      throws BufferNotDirectException {
    return new MutableDirectBufferVal(buffer, autoRefresh);
  }

  private final boolean autoRefresh;

  /**
   * Last buffer address written to <code>MDB_val</code>.
   */
  private long lastAddress = 0;

  /**
   * Last buffer capacity written to <code>MDB_val</code>.
   */
  private long lastCapacity = 0;

  private MutableDirectBuffer mdb;

  private MutableDirectBufferVal(final MutableDirectBuffer buffer,
                                 final boolean autoRefresh)
      throws BufferNotDirectException {
    super();
    this.autoRefresh = autoRefresh;
    wrap(buffer);
  }

  /**
   * Returns the internal buffer currently wrapped by this instance.
   *
   * @return the buffer (never null)
   */
  public MutableDirectBuffer buffer() {
    return mdb;
  }

  @Override
  public long dataAddress() {
    return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_DATA);
  }

  @Override
  public void refresh() {
    mdb.wrap(dataAddress(), (int) size());
  }

  @Override
  public long size() {
    return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE);
  }

  /**
   * Set the internal buffer to the passed instance.
   *
   * @param buffer instance to use (required; must be direct)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public void wrap(final MutableDirectBuffer buffer) throws
      BufferNotDirectException {
    if (SHOULD_CHECK) {
      requireNonNull(buffer);
      requireDirectBuffer(buffer.byteBuffer());
    }
    mdb = buffer;
  }

  @Override
  void dirty() {
    if (autoRefresh) {
      refresh();
    }
  }

  @Override
  void set() {
    final long newAddress = mdb.addressOffset();
    final long newCapacity = mdb.capacity();
    if (newAddress == lastAddress && newCapacity == lastCapacity) {
      return;
    }
    lastAddress = newAddress;
    lastCapacity = newCapacity;
    UNSAFE.putLong(ptrAddress + STRUCT_FIELD_OFFSET_DATA, newAddress);
    UNSAFE.putLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE, newCapacity);
  }

}
