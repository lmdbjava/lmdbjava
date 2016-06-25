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
 * <p>
 * The proxy is passed to the {@link Env#create(org.lmdbjava.BufferProxy)}
 * method and is subsequently used by every {@link Txn}, {@link Dbi} and
 * {@link Cursor} associated with the {@link Env}.
 *
 * @param <T> buffer type
 */
public abstract class BufferProxy<T> {

  /**
   * Size of a <code>MDB_val</code> pointer in bytes.
   */
  protected static final int MDB_VAL_STRUCT_SIZE = BYTES * 2;

  /**
   * Offset from a pointer of the <code>MDB_val.mv_data</code> field.
   */
  protected static final int STRUCT_FIELD_OFFSET_DATA = BYTES;

  /**
   * Offset from a pointer of the <code>MDB_val.mv_size</code> field.
   */
  protected static final int STRUCT_FIELD_OFFSET_SIZE = 0;

  /**
   * Allocate a new buffer suitable for read-write use.
   *
   * @param bytes the size of the buffer
   * @return a writable buffer suitable for use with buffer-requiring methods
   */
  protected abstract T allocate(int bytes);

  /**
   * Deallocate a buffer that was previously provided by {@link #allocate(int)}.
   *
   * @param buff the buffer to deallocate (required)
   */
  protected abstract void deallocate(T buff);

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed
   * buffer.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected abstract void in(T buffer, Pointer ptr, long ptrAddr);

  /**
   * Called when the <code>MDB_val</code> may have changed and the passed buffer
   * should be modified to reflect the new <code>MDB_val</code>.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected abstract void out(T buffer, Pointer ptr, long ptrAddr);

}
