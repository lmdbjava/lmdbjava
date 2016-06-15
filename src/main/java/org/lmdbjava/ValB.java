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

import static java.lang.Long.BYTES;
import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import static org.lmdbjava.Library.RUNTIME;

/**
 * Equivalent of a C <code>MDB_val</code>.
 * <p>
 * This class is designed to efficiently support a variety of Java buffer
 * implementations (including <code>ByteBuffer</code>, third-party Unsafe-based
 * buffers, long-indexed buffers) while ensuring flyweight patterns can be
 * applied and usability is not diminished.
 */
public abstract class ValB {

  private static final int MDB_VAL_STRUCT_SIZE = BYTES * 2;
  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();
  /**
   * Offset from {@link #ptrAddress} where the <code>MBV_val.mv_data</code>
   * field can be read.
   */
  protected static final int STRUCT_FIELD_OFFSET_DATA = BYTES;

  /**
   * Offset from {@link #ptrAddress} where the <code>MDB_val.mv_size</code>
   * field can be read.
   */
  protected static final int STRUCT_FIELD_OFFSET_SIZE = 0;
  /**
   * The actual pointer unpinning this instance. It has protected visibility for
   * implementations that may require its methods to get/set the native memory
   * fields. Implementations must not use any other methods.
   */
  protected final Pointer ptr;
  /**
   * Absolute native memory address where the <code>MDB_val</code> structure
   * underpinning this instance has been allocated.
   */
  protected final long ptrAddress;

  protected ValB() {
    ptr = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrAddress = ptr.address();
  }

  /**
   * Fetch the current <code>MDB_val.mv_data</code> from native memory.
   * <p>
   * In general an end user should not need to call this method. However, it may
   * be more efficient if only the address is required (eg for passing to
   * another API that accepts absolute memory addresses).
   *
   * @return the absolute memory address where the actual data begins
   */
  public abstract long dataAddress();

  /**
   * Fetch the current <code>MDB_val.mv_size</code> from native memory.
   * <p>
   * In general an end user should not need to call this method. However, it may
   * be more efficient if only the size is required (eg for iterating over
   * cursors and summing the total size or finding specifically-sized records).
   *
   * @return the size in bytes of the data
   */
  public abstract long size();

  /**
   * Sets the <code>MDB_val.mv_size</code> and <code>MDB_val.mv_data</code>
   * fields to reflect the current Java-side buffer address and size. This
   * method is invoked by LmdbJava just before it invokes LMDB functions that
   * will read existing <code>MDB_val</code> values (for efficiency reasons it
   * does not invoke this method before it invokes LMDB functions that merely
   * write new values to the passed <code>MDB_val</code>).
   */
  abstract void set();

}
