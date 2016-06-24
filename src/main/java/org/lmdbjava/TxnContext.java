package org.lmdbjava;

import jnr.ffi.Pointer;
import jnr.ffi.provider.MemoryManager;
import static org.lmdbjava.BufferProxy.MDB_VAL_STRUCT_SIZE;
import static org.lmdbjava.Library.RUNTIME;

class TxnContext<T> {

  static final MemoryManager MEM_MGR = RUNTIME.getMemoryManager();
  boolean closed;
  final BufferProxyFactory<T> factory;
  final BufferProxy<T> key;

  final Pointer ptrKey;
  final long ptrKeyAddr;
  final Pointer ptrVal;
  final long ptrValAddr;
  final Txn txn;
  final BufferProxy<T> val;

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

  public void keyIn(T key) {
    this.key.in(key, ptrKey, ptrKeyAddr);
  }

  public void keyOut() {
    key.out(ptrKey, ptrKeyAddr);
  }

  public void valIn(T val) {
    this.val.in(val, ptrVal, ptrValAddr);
  }

  public void valOut() {
    val.out(ptrVal, ptrValAddr);
  }

  void close() {
    if (closed) {
      return;
    }
    factory.deallocate(key);
    factory.deallocate(val);
    closed = true;
  }
}
