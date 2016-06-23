package org.lmdbjava;

public interface BufferProxyFactory<T> {
  BufferProxy<T> allocate();
  void deallocate(BufferProxy<T> proxy);
}
