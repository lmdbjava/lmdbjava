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

import static java.lang.Long.BYTES;
import java.nio.Buffer;
import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import static org.lmdbjava.BufferMutators.MUTATOR;
import static org.lmdbjava.BufferMutators.requireDirectBuffer;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.RUNTIME;
import org.lmdbjava.LmdbException.BufferNotDirectException;

/**
 * Methods for safely interacting with <code>MDB_val</code> pointers.
 * <p>
 * While JNR offers <code>struct</code> abstraction, benchmarking has shown this
 * to be less efficient than simply using offset-based pointers. This class
 * ensures the correct struct offsets are used with {@link BufferMutator}
 * operations.
 */
final class ValueBuffers {

  private static final int MDB_VAL_STRUCT_SIZE = BYTES * 2;
  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();
  private static final int STRUCT_FIELD_OFFSET_DATA = BYTES;
  private static final int STRUCT_FIELD_OFFSET_SIZE = 0;

  /**
   * Allocate memory to store a <code>MDB_val</code> and return a pointer to
   * that memory.
   *
   * @return the allocated location
   */
  static Pointer allocateMdbVal() {
    return MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
  }

  static Pointer allocateMdbVal(final Buffer src) throws
      BufferNotDirectException {
    if (src == null) {
      return null;
    }
    final Pointer dest = allocateMdbVal();
    setPointerToBuffer(src, dest);
    return dest;
  }

  static long setBufferToPointer(final Pointer src, final Buffer dest) throws
      BufferNotDirectException {
    if (SHOULD_CHECK) {
      assert src.isDirect();
      requireDirectBuffer(dest);
    }
    final long size = src.getLong(STRUCT_FIELD_OFFSET_SIZE);
    final long data = src.getAddress(STRUCT_FIELD_OFFSET_DATA);
    MUTATOR.modify(dest, data, (int) size);
    return size;
  }

  static void setPointerToBuffer(final Buffer src, final Pointer dest) throws
      BufferNotDirectException {
    if (SHOULD_CHECK) {
      requireDirectBuffer(src);
      assert dest.isDirect();
    }
    final long size = src.capacity();
    final long data = ((sun.nio.ch.DirectBuffer) src).address();
    dest.putLong(STRUCT_FIELD_OFFSET_SIZE, size);
    dest.putLong(STRUCT_FIELD_OFFSET_DATA, data);
  }

  private ValueBuffers() {
  }

}
