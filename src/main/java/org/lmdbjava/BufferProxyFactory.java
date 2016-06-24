package org.lmdbjava;

/**
 *
 * @param <T>
 */
public interface BufferProxyFactory<T> {

  /**
   *
   * @return
   */
  BufferProxy<T> allocate();

  /**
   *
   * @param proxy
   */
  void deallocate(BufferProxy<T> proxy);
}
