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

import static java.lang.Integer.BYTES;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import org.junit.Test;
import static org.lmdbjava.ByteBufferVal.requireDirectBuffer;
import org.lmdbjava.LmdbException.BufferNotDirectException;

public class ByteBufferValTest {

  @Test
  public void directBufferAllowed() throws Exception {
    requireDirectBuffer(allocateDirect(BYTES));
  }

  @Test(expected = BufferNotDirectException.class)
  public void javaBufferRejected() throws Exception {
    requireDirectBuffer(allocate(BYTES));
  }
}
