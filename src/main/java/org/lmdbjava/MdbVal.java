package org.lmdbjava;

import java.nio.ByteBuffer;

import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.ADDRESS;
import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.CAPACITY;
import static org.lmdbjava.BufferMutators.UnsafeBufferMutator.UNSAFE;

public class MdbVal {
  static final long MDB_VAL_SIZE_OFFSET = 0;
  static final long MDB_VAL_DATA_OFFSET = 8;
  private long address;
  private long bufferAddress;
  private long mdbValAddress;
  private int size;
  private ByteBuffer buffer;

  public MdbVal(ByteBuffer buffer) {
    wrap(buffer);
  }

  public MdbVal(long address, int size) {
    wrap(address, size);
  }

  public void wrap(ByteBuffer buffer) {
    this.buffer = buffer;
    this.address = ((sun.nio.ch.DirectBuffer) buffer).address();
    this.bufferAddress = address;
    this.size = buffer.capacity();
  }

  public void wrap(long address, int size) {
    this.address = address;
    this.size = size;
  }

  public ByteBuffer getByteBuffer() {
    if (mdbValAddress != 0) {
      setMdbValSizeAndAddress();
      mdbValAddress = 0;
      if (buffer != null) {
        UNSAFE.putLong(buffer, ADDRESS, address);
        UNSAFE.putInt(buffer, CAPACITY, size);
        buffer.limit(size);
        buffer.rewind();
      }
    }
    return buffer;
  }

  public long getAddress() {
    if (mdbValAddress != 0) {
      setMdbValSizeAndAddress();
      mdbValAddress = 0;
    }
    return address;
  }

  public int getSize() {
    if (mdbValAddress != 0) {
      setMdbValSizeAndAddress();
      mdbValAddress = 0;
    }
    return size;
  }

  void wrapOutMdbValStruct(long address) {
    this.mdbValAddress = address;
  }

  void wrapInMdbValStruct(long address) {
    UNSAFE.putLong(address + MDB_VAL_SIZE_OFFSET, size);
    UNSAFE.putLong(address + MDB_VAL_DATA_OFFSET, this.address);
  }

  private void setMdbValSizeAndAddress() {
    this.size = (int) UNSAFE.getLong(mdbValAddress + MDB_VAL_SIZE_OFFSET);
    this.address = UNSAFE.getLong(mdbValAddress + MDB_VAL_DATA_OFFSET);
  }
}
