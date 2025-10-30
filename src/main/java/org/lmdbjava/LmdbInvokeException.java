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

/** Superclass for all LmdbJava custom exceptions. */
public class LmdbInvokeException extends LmdbException {

  private static final long serialVersionUID = 1L;

  private final String lmdbMethod;

  /**
   * Constructs an instance with the provided detailed message.
   *
   * @param message the detail message
   */
  public LmdbInvokeException(final String lmdbMethod, final String message) {
    super("Error calling '" + lmdbMethod + "' " + message);
    this.lmdbMethod = lmdbMethod;
  }

  /**
   * Constructs an instance with the provided detailed message and cause.
   *
   * @param message the detail message
   * @param cause original cause
   */
  public LmdbInvokeException(final String lmdbMethod,
                             final String message,
                             final Throwable cause) {
    super("Error calling '" + lmdbMethod + "' " + message, cause);
    this.lmdbMethod = lmdbMethod;
  }

  public String getLmdbMethod() {
    return lmdbMethod;
  }
}
