/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static java.lang.Long.BYTES;
import static org.lmdbjava.DbiFlags.MDB_INTEGERKEY;
import static org.lmdbjava.DbiFlags.MDB_UNSIGNEDKEY;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;

import java.util.Comparator;
import jnr.ffi.Pointer;

/**
 * The strategy for mapping memory address to a given buffer type.
 *
 * <p>The proxy is passed to the {@link Env#create(org.lmdbjava.BufferProxy)} method and is
 * subsequently used by every {@link Txn}, {@link Dbi} and {@link Cursor} associated with the {@link
 * Env}.
 *
 * @param <T> buffer type
 */
public abstract class BufferProxy<T> {

  /** Size of a <code>MDB_val</code> pointer in bytes. */
  protected static final int MDB_VAL_STRUCT_SIZE = BYTES * 2;

  /** Offset from a pointer of the <code>MDB_val.mv_data</code> field. */
  protected static final int STRUCT_FIELD_OFFSET_DATA = BYTES;

  /** Offset from a pointer of the <code>MDB_val.mv_size</code> field. */
  protected static final int STRUCT_FIELD_OFFSET_SIZE = 0;

  /** Explicitly-defined default constructor to avoid warnings. */
  protected BufferProxy() {}

  /**
   * Allocate a new buffer suitable for passing to {@link #out(java.lang.Object, jnr.ffi.Pointer,
   * long)}.
   *
   * @return a buffer for passing to the <code>out</code> method
   */
  protected abstract T allocate();

  /**
   * Deallocate a buffer that was previously provided by {@link #allocate()}.
   *
   * @param buff the buffer to deallocate (required)
   */
  protected abstract void deallocate(T buff);

  /**
   * Obtain a copy of the bytes contained within the passed buffer.
   *
   * @param buffer a non-null buffer created by this proxy instance
   * @return a copy of the bytes this buffer is currently representing
   */
  protected abstract byte[] getBytes(T buffer);

  /**
   * Get a suitable default {@link Comparator} given the provided flags.
   *
   * <p>The provided comparator must strictly match the lexicographical order of keys in the native
   * LMDB database.
   *
   * @param flags for the database
   * @return a comparator that can be used (never null)
   */
  protected Comparator<T> getComparator(DbiFlags... flags) {
    final int intFlag = mask(flags);

    return isSet(intFlag, MDB_INTEGERKEY) || isSet(intFlag, MDB_UNSIGNEDKEY)
        ? getUnsignedComparator()
        : getSignedComparator();
  }

  /**
   * Get a suitable default {@link Comparator} to compare numeric key values as signed.
   *
   * @return a comparator that can be used (never null)
   */
  protected abstract Comparator<T> getSignedComparator();

  /**
   * Get a suitable default {@link Comparator} to compare numeric key values as unsigned.
   *
   * @return a comparator that can be used (never null)
   */
  protected abstract Comparator<T> getUnsignedComparator();

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed buffer. This buffer
   * will have been created by end users, not {@link #allocate()}.
   *
   * @param buffer the buffer to write to <code>MDB_val</code>
   * @param ptr the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected abstract void in(T buffer, Pointer ptr, long ptrAddr);

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed buffer.
   *
   * @param buffer the buffer to write to <code>MDB_val</code>
   * @param size the buffer size to write to <code>MDB_val</code>
   * @param ptr the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected abstract void in(T buffer, int size, Pointer ptr, long ptrAddr);

  /**
   * Called when the <code>MDB_val</code> may have changed and the passed buffer should be modified
   * to reflect the new <code>MDB_val</code>.
   *
   * @param buffer the buffer to write to <code>MDB_val</code>
   * @param ptr the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   * @return the buffer for <code>MDB_val</code>
   */
  protected abstract T out(T buffer, Pointer ptr, long ptrAddr);

  /**
   * Create a new {@link KeyVal} to hold pointers for this buffer proxy.
   *
   * @return a non-null key value holder
   */
  final KeyVal<T> keyVal() {
    return new KeyVal<>(this);
  }
}
