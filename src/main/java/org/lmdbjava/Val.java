package org.lmdbjava;

import java.nio.ByteBuffer;

import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.ADDRESS;
import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.CAPACITY;
import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.UNSAFE;

public class Val {
  static final long MDB_VAL_SIZE_OFFSET = 0;
  static final long MDB_VAL_DATA_OFFSET = 8;
  private long address;
  private long mdbValDataAddress;
  private long mdbValSizeAddress;
  private int size;
  private ByteBuffer buffer;

  public Val(ByteBuffer buffer) {
    wrap(buffer);
  }

  public Val(long address, int size) {
    wrap(address, size);
  }

  public void wrap(ByteBuffer buffer) {
    this.buffer = buffer;
    this.address = ((sun.nio.ch.DirectBuffer) buffer).address();
    this.size = buffer.capacity();
  }

  public void wrap(long address, int size) {
    this.address = address;
    this.size = size;
  }

  public ByteBuffer getByteBuffer() {
    if (mdbValDataAddress != 0) {
      if (buffer != null) {
        UNSAFE.putLong(buffer, ADDRESS, getAddress());
        UNSAFE.putInt(buffer, CAPACITY, getSize());
        buffer.clear();
      }
    }
    return buffer;
  }

  public long getAddress() {
    if (mdbValDataAddress != 0) {
      this.address = UNSAFE.getLong(mdbValDataAddress);
      mdbValDataAddress = 0;
    }
    return address;
  }

  public int getSize() {
    if (mdbValSizeAddress != 0) {
      this.size = (int) UNSAFE.getLong(mdbValSizeAddress);
      mdbValSizeAddress = 0;
    }
    return size;
  }

  void wrapOutMdbValStruct(long address) {
    this.mdbValSizeAddress = address + MDB_VAL_SIZE_OFFSET;
    this.mdbValDataAddress = address + MDB_VAL_DATA_OFFSET;
  }

  void wrapInMdbValStruct(long address) {
    UNSAFE.putLong(address + MDB_VAL_SIZE_OFFSET, size);
    UNSAFE.putLong(address + MDB_VAL_DATA_OFFSET, this.address);
  }
}
