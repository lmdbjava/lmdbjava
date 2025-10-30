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

import org.lmdbjava.Lmdb.MDB_arr_val;
import org.lmdbjava.Lmdb.MDB_val;

import java.lang.foreign.Arena;

import static java.util.Objects.requireNonNull;

/**
 * Represents off-heap memory holding a key and value pair.
 *
 * @param <T> buffer type
 */
final class KeyVal<T> implements AutoCloseable {

  private boolean closed;
  private T k;
  private final BufferProxy<T> proxy;
  private final MDB_arr_val ptrArray;
  private final MDB_val ptrKey;
  private final MDB_val ptrVal;
  private T v;

  KeyVal(final Arena arena,
         final BufferProxy<T> proxy) {
    requireNonNull(proxy);
    this.proxy = proxy;
    this.k = proxy.allocate();
    this.v = proxy.allocate();
    ptrKey = new MDB_val(arena);
    ptrArray = new MDB_arr_val(arena);
    ptrVal = new MDB_val(ptrArray.segment());
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
    proxy.in(key, ptrKey);
  }

  T keyOut() {
    k = proxy.out(ptrKey);
    return k;
  }

  MDB_val pointerKey() {
    return ptrKey;
  }

  MDB_val pointerVal() {
    return ptrVal;
  }

  T val() {
    return v;
  }

  void valIn(final T val) {
    proxy.in(val, ptrVal);
  }

  void valIn(final int size) {
    proxy.in(v, size, ptrVal);
  }

  /**
   * Prepares an array suitable for presentation as the data argument to a <code>MDB_MULTIPLE</code>
   * put.
   *
   * <p>The returned array is equivalent of two <code>MDB_val</code>s as follows:
   *
   * <ul>
   *   <li>ptrVal1.size = size of each individual data element
   *   <li>ptrVal1.data = pointer to the data address of passed buffer
   *   <li>ptrVal2.size = number of data elements (as passed to this method)
   *   <li>ptrVal2.data = unused
   * </ul>
   *
   * @param val      a user-provided buffer with data elements (required)
   * @param elements number of data elements the user has provided
   * @return a properly-prepared pointer to an array for the operation
   */
  MDB_val valInMulti(final T val, final int elements) {
    ptrArray.mvSize2(elements); // ptrVal2.size
    proxy.in(val, ptrVal); // ptrVal1.data
    final long totalBufferSize = ptrVal.mvSize();
    final long elemSize = totalBufferSize / elements;
    ptrArray.mvSize1(elemSize); // ptrVal1.size
    // Return ptrVal as both array and value share same address.
    return ptrVal;
  }

  T valOut() {
    v = proxy.out(ptrVal);
    return v;
  }
}
