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

import java.nio.ByteBuffer;

public class ByteBufferUtils {

  private ByteBufferUtils() {
    // static util methods only
  }

  /**
   * Creates a new direct {@link ByteBuffer} from the input {@link ByteBuffer}. The bytes from
   * position() to limit() will be copied into a newly allocated buffer. The new buffer will be
   * flipped to set its position read for get operations
   */
  public static ByteBuffer copyToDirectBuffer(final ByteBuffer input) {
    final ByteBuffer output = ByteBuffer.allocateDirect(input.remaining());
    output.put(input);
    output.flip();
    input.rewind();
    return output;
  }
}
