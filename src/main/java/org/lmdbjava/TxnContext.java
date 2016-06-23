package org.lmdbjava;

import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;

import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import static org.lmdbjava.Library.RUNTIME;

class TxnContext<T> {
  static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();
  boolean closed;
  final Txn txn;
  final BufferProxyFactory<T> factory;
  final BufferProxy<T> key;
  final BufferProxy<T> val;

  final Pointer ptrKey;
  final long ptrKeyAddr;
  final Pointer ptrVal;
  final long ptrValAddr;

  TxnContext(Txn tx, BufferProxyFactory<T> factory) {
    this.txn = tx;
    this.factory = factory;
    this.key = factory.allocate();
    this.val = factory.allocate();
    ptrKey = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrKeyAddr = ptrKey.address();
    ptrVal = MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false);
    ptrValAddr = ptrVal.address();
  }

  void close() {
    if (closed) {
      return;
    }
    factory.deallocate(key);
    factory.deallocate(val);
    closed = true;
  }

  public void keyIn(T key) {
    this.key.in(key, ptrKey, ptrKeyAddr);
  }

  public void valIn(T val) {
    this.val.in(val, ptrVal, ptrValAddr);
  }

  public void keyOut() {
    key.out(ptrKey, ptrKeyAddr);
  }

  public void valOut() {
    val.out(ptrVal, ptrValAddr);
  }
}
