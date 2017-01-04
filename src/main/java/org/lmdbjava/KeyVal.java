/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
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
    ptrVal = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
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

  T valOut() {
    v = proxy.out(v, ptrVal, ptrValAddr);
    return v;
  }

}
