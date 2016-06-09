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

import java.nio.ByteBuffer;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import static org.lmdbjava.BufferMutators.MUTATOR;
import static org.lmdbjava.BufferMutators.requireDirectBuffer;
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.runtime;
import org.lmdbjava.LmdbException.BufferNotDirectException;

final class ValueBuffers {

  static MDB_val createVal(final ByteBuffer bb) throws BufferNotDirectException {
    requireDirectBuffer(bb);
    final MDB_val val = new MDB_val(runtime);
    val.size.set(bb.limit());
    val.data.set(new ByteBufferMemoryIO(runtime, bb));
    return val;
  }

  static void wrap(final ByteBuffer bb, final MDB_val val) throws
      BufferNotDirectException {
    requireDirectBuffer(bb);
    final long address = val.data.get().address();
    final int capacity = (int) val.size.get();
    MUTATOR.modify(bb, address, capacity);
  }

  private ValueBuffers() {
  }

}
