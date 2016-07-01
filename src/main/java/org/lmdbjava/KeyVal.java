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

/**
 * Holder for a key and value pair.
 *
 * @param <T> buffer type
 */
public class KeyVal<T> {

  /**
   *
   */
  public T key;

  /**
   *
   */
  public T val;

  /**
   *
   * @param key
   * @param val
   */
  public KeyVal(T key, T val) {
    this.key = key;
    this.val = val;
  }

  /**
   *
   * @param key
   */
  public void setKey(T key) {
    this.key = key;
  }

  /**
   *
   * @param val
   */
  public void setVal(T val) {
    this.val = val;
  }

  /**
   *
   * @param key
   * @param val
   */
  public void wrap(T key, T val) {
    this.key = key;
    this.val = val;
  }
}
