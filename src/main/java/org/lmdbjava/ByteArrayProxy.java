package org.lmdbjava;

import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;

import static org.lmdbjava.Library.RUNTIME;

/**
 * Byte array proxy.
 *
 * {@link Env#byteArray()}
 */
public class ByteArrayProxy extends BufferProxy<byte[]> {
  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();

  /**
   * The byte array proxy. Guaranteed to never be null.
   */
  public static final BufferProxy<byte[]> PROXY_BA = new ByteArrayProxy();

  @Override
  protected byte[] allocate() {
    return new byte[0];
  }

  @Override
  protected void deallocate(byte[] buff) {
  }

  @Override
  protected void in(byte[] buffer, Pointer ptr, long ptrAddr) {
    Pointer pointer = MEM_MGR.allocateDirect(buffer.length);
    pointer.put(0, buffer, 0, buffer.length);
    ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.length);
    ptr.putLong(STRUCT_FIELD_OFFSET_DATA, pointer.address());
  }

  @Override
  protected void in(byte[] buffer, int size, Pointer ptr, long ptrAddr) {

  }

  @Override
  protected byte[] out(byte[] buffer, Pointer ptr, long ptrAddr) {
    final long addr = ptr.getLong(STRUCT_FIELD_OFFSET_DATA);
    final int size = (int) ptr.getLong(STRUCT_FIELD_OFFSET_SIZE);
    Pointer pointer = MEM_MGR.newPointer(addr, size);
    byte[] bytes = new byte[size];
    pointer.get(0, bytes, 0, size);
    return bytes;
  }
}
