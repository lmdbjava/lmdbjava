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
import jnr.ffi.Pointer;

/**
 * The strategy for mapping memory address to a given buffer type.
 *
 * @param <T> type of buffer managed by this proxy
 */
public interface BufferProxy<T> {

  /**
   * Size of a <code>MDB_val</code> pointer in bytes.
   */
  int MDB_VAL_STRUCT_SIZE = BYTES * 2;

  /**
   * Offset from a pointer of the <code>MDB_val.mv_data</code> field.
   */
  int STRUCT_FIELD_OFFSET_DATA = BYTES;

  /**
   * Offset from a pointer of the <code>MDB_val.mv_size</code> field.
   */
  int STRUCT_FIELD_OFFSET_SIZE = 0;

  /**
   * Called when the <code>MDB_val</code> may have changed and the passed buffer
   * should be modified to reflect the new <code>MDB_val</code>.
   *
   * @param buffer the buffer to modify to reflect the <code>MDB_val</code>
   * @param ptr      the pointer to the <code>MDB_val</code>
   * @param ptrAddr  the address of the <code>MDB_val</code> pointer
   */
  void out(Pointer ptr, long ptrAddr);

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed
   * buffer.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  void in(T buffer, Pointer ptr, long ptrAddr);

  /**
   * Allocate a new buffer suitable for read-write use.
   *
   * @param bytes the size of the buffer
   * @return a writable buffer suitable for use with buffer-requiring methods
   */
  T allocate(int bytes);

  T buffer();

}
