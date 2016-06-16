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

import java.nio.ByteBuffer;
import static org.lmdbjava.BufferMutators.MUTATOR;
import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.UNSAFE;
import static org.lmdbjava.BufferMutators.requireDirectBuffer;
import static org.lmdbjava.Env.SHOULD_CHECK;
import org.lmdbjava.LmdbException.BufferNotDirectException;

/**
 * <code>ByteBuffer</code> value.
 */
public final class ByteBufferValB extends ValB {

  private final boolean autoRefresh;
  private ByteBuffer bb;
  private long bbAddress;

  /**
   * Create a new instance that uses the passed <code>ByteBuffer</code> to get
   * and set bytes from LMDB. Auto-refresh is enabled, meaning the user does not
   * ever need to invoke {@link #refresh()} (ie the buffer will always reflect
   * the <code>MDB_val</code> when modified by the LMDB C API).
   *
   * @param buffer instance to use
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public ByteBufferValB(final ByteBuffer buffer) throws BufferNotDirectException {
    this(buffer, true);
  }

  /**
   * Create a new instance that uses the passed <code>ByteBuffer</code> to get
   * and set bytes from LMDB.
   *
   * @param buffer      instance to use
   * @param autoRefresh automatically refresh the buffer when {@link #dirty()}
   *                    is called
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public ByteBufferValB(final ByteBuffer buffer, final boolean autoRefresh)
      throws BufferNotDirectException {
    super();
    wrap(buffer);
    this.autoRefresh = autoRefresh;
  }

  /**
   * Returns the internal <code>ByteBuffer</code> currently wrapped by this
   * instance.
   *
   * @return the buffer (never null)
   */
  public ByteBuffer buffer() {
    return bb;
  }

  @Override
  public long dataAddress() {
    return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_DATA);
  }

  /**
   * Modify the internal <code>ByteBuffer</code> so it now points at the current
   * <code>MDB_val</code> address and capacity.
   * <p>
   * The result is undefined if the <code>MDB_val</code> has not been populated
   * with a valid value (eg an LdmbJava method was not invoked beforehand).
   */
  @Override
  public void refresh() {
    MUTATOR.modify(bb, dataAddress(), (int) size());
  }

  @Override
  public long size() {
    return UNSAFE.getLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE);
  }

  /**
   * Set the internal <code>ByteBuffer</code> to the passed instance.
   *
   * @param buffer instance to use (required; must be direct)
   * @throws BufferNotDirectException if a passed buffer is invalid
   */
  public void wrap(final ByteBuffer buffer) throws BufferNotDirectException {
    if (SHOULD_CHECK) {
      requireDirectBuffer(buffer);
    }
    this.bb = buffer;
    this.bbAddress = ((sun.nio.ch.DirectBuffer) buffer).address();
  }

  @Override
  void dirty() {
    if (autoRefresh) {
      refresh();
    }
  }

  @Override
  void set() {
    UNSAFE.putLong(ptrAddress + STRUCT_FIELD_OFFSET_SIZE, bb.capacity());
    UNSAFE.putLong(ptrAddress + STRUCT_FIELD_OFFSET_DATA, bbAddress);
  }

}
