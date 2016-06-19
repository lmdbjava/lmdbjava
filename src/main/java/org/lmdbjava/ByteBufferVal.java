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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import static org.lmdbjava.Env.SHOULD_CHECK;
import org.lmdbjava.LmdbException.BufferNotDirectException;

/**
 * {@link ByteBuffer} value.
 * <p>
 * Use {@link ByteBufferVals#forBuffer(java.nio.ByteBuffer)} to obtain a
 * concrete implementation.
 */
public abstract class ByteBufferVal extends Val {

  static void requireDirectBuffer(final Buffer buffer) throws
      BufferNotDirectException {
    if (!buffer.isDirect()) {
      throw new BufferNotDirectException();
    }
  }

  private final boolean autoRefresh;

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
  }

  @Override
  final void dirty() {
    if (autoRefresh) {
      refresh();
    }
  }

}
