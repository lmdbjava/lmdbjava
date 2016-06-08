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
   * The passed buffer can be of any address or capacity.
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
