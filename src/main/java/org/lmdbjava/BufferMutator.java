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

import java.nio.Buffer;

/**
 * Efficiently modifies the internal limits and offsets of a {@link Buffer}.
 */
interface BufferMutator {

  /**
   * Modifies the passed direct <code>Buffer</code> to point at the indicated
   * memory address and size.
   * <p>
   * The passed buffer can be of any address or capacity, but the buffer must be
   * direct. The result of presenting a non-direct buffer is undefined.
   * <p>
   * {@link Buffer#clear() } will be invoked automatically (this does not clear
   * the buffer, but simply resets its limits, position and mark).
   *
   * @param buffer   a direct (off-heap) buffer
   * @param address  the new memory address of the buffer
   * @param capacity the new capacity of the buffer
   */
  void modify(Buffer buffer, long address, int capacity);
}
