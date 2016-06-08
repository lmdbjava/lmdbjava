package org.lmdbjava;

import java.nio.ByteBuffer;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import static org.lmdbjava.BufferMutators.MUTATOR;
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.runtime;

final class ValueBuffers {

  static MDB_val createVal(final ByteBuffer bb) {
    final MDB_val val = new MDB_val(runtime);
    val.size.set(bb.limit());
    val.data.set(new ByteBufferMemoryIO(runtime, bb));
    return val;
  }

  static void wrap(final ByteBuffer buffer, final MDB_val val) {
    final long address = val.data.get().address();
    final int capacity = (int) val.size.get();
    MUTATOR.modify(buffer, address, capacity);
  }

  private ValueBuffers() {
  }

}
