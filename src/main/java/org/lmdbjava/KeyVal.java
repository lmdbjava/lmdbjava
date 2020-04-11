/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
 * %%
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
 * #L%
 */

package org.lmdbjava;

import static java.util.Objects.requireNonNull;
import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import static org.lmdbjava.BufferProxy.STRUCT_FIELD_OFFSET_SIZE;
import static org.lmdbjava.Library.RUNTIME;

/**
 * Represents off-heap memory holding a key and value pair.
 *
 * @param <T> buffer type
 */
final class KeyVal<T> implements AutoCloseable {

  private static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();
  private boolean closed;
  private T k;
  private final BufferProxy<T> proxy;
  private final Pointer ptrArray;
  private final Pointer ptrKey;
  private final long ptrKeyAddr;
  private final Pointer ptrVal;
  private final long ptrValAddr;
  private T v;

  KeyVal(final BufferProxy<T> proxy) {
    requireNonNull(proxy);
    this.proxy = proxy;
    this.k = proxy.allocate();
    this.v = proxy.allocate();
    ptrKey = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrKeyAddr = ptrKey.address();
    ptrArray = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE * 2, false);
    ptrVal = ptrArray.slice(0, MDB_VAL_STRUCT_SIZE);
    ptrValAddr = ptrVal.address();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    proxy.deallocate(k);
    proxy.deallocate(v);
  }

  T key() {
    return k;
  }

  void keyIn(final T key) {
    proxy.in(key, ptrKey, ptrKeyAddr);
  }

  T keyOut() {
    k = proxy.out(k, ptrKey, ptrKeyAddr);
    return k;
  }

  Pointer pointerKey() {
    return ptrKey;
  }

  Pointer pointerVal() {
    return ptrVal;
  }

  T val() {
    return v;
  }

  void valIn(final T val) {
    proxy.in(val, ptrVal, ptrValAddr);
  }

  void valIn(final int size) {
    proxy.in(v, size, ptrVal, ptrValAddr);
  }

  /**
   * Prepares an array suitable for presentation as the data argument to a
   * <code>MDB_MULTIPLE</code> put.
   *
   * <p>
   * The returned array is equivalent of two <code>MDB_val</code>s as follows:
   *
   * <ul>
   * <li>ptrVal1.data = pointer to the data address of passed buffer</li>
   * <li>ptrVal1.size = size of each individual data element</li>
   * <li>ptrVal2.data = unused</li>
   * <li>ptrVal2.size = number of data elements (as passed to this method)</li>
   * </ul>
   *
   * @param val      a user-provided buffer with data elements (required)
   * @param elements number of data elements the user has provided
   * @return a properly-prepared pointer to an array for the operation
   */
  Pointer valInMulti(final T val, final int elements) {
    final long ptrVal2SizeOff = MDB_VAL_STRUCT_SIZE + STRUCT_FIELD_OFFSET_SIZE;
    ptrArray.putLong(ptrVal2SizeOff, elements); // ptrVal2.size
    proxy.in(val, ptrVal, ptrValAddr); // ptrVal1.data
    final long totalBufferSize = ptrVal.getLong(STRUCT_FIELD_OFFSET_SIZE);
    final long elemSize = totalBufferSize / elements;
    ptrVal.putLong(STRUCT_FIELD_OFFSET_SIZE, elemSize); // ptrVal1.size

    return ptrArray;
  }

  T valOut() {
    v = proxy.out(v, ptrVal, ptrValAddr);
    return v;
  }

}
