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

import static java.lang.Boolean.getBoolean;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Provides access to Unsafe.
 */
final class UnsafeAccess {

  /**
   * Java system property name that can be set to disable unsafe.
   */
  public static final String DISABLE_UNSAFE_PROP = "lmdbjava.disable.unsafe";

  /**
   * Indicates whether unsafe use is allowed.
   */
  public static final boolean ALLOW_UNSAFE = !getBoolean(DISABLE_UNSAFE_PROP);

  /**
   * The actual unsafe. Guaranteed to be non-null if this class can access
   * unsafe and {@link #ALLOW_UNSAFE} is true. In other words, this entire class
   * will fail to initialize if unsafe is unavailable. This avoids callers from
   * needing to deal with null checks.
   */
  static final Unsafe UNSAFE;
  /**
   * Unsafe field name (used to reflectively obtain the unsafe instance).
   */
  private static final String FIELD_NAME_THE_UNSAFE = "theUnsafe";

  static {
    if (!ALLOW_UNSAFE) {
      throw new LmdbException("Unsafe disabled by user");
    }
    try {
      final Field field = Unsafe.class.getDeclaredField(FIELD_NAME_THE_UNSAFE);
      field.setAccessible(true);
      UNSAFE = (Unsafe) field.get(null);
    } catch (final NoSuchFieldException | SecurityException
                       | IllegalArgumentException | IllegalAccessException e) {
      throw new LmdbException("Unsafe unavailable", e);
    }
  }

  private UnsafeAccess() {
  }

}
