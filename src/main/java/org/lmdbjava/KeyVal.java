package org.lmdbjava;

/**
 * Holder for a key and value pair.
 *
 * @param <T> buffer type
 */
public class KeyVal<T> {
  public T key;
  public T val;

  public KeyVal(T key, T val) {
    this.key = key;
    this.val = val;
  }

  public void setKey(T key) {
    this.key = key;
  }

  public void setVal(T val) {
    this.val = val;
  }

  public void wrap(T key, T val) {
    this.key = key;
    this.val = val;
  }
}
