/*
 * Copyright © 2016-2026 The LmdbJava Open Source Project
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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class KeyValTest {

  @Test
  void testClose() {
    //noinspection unchecked
    final BufferProxy<ByteBuffer> mockBufferProxy =
        (BufferProxy<ByteBuffer>) Mockito.mock(BufferProxy.class);
    final KeyVal<ByteBuffer> keyVal = new KeyVal<>(mockBufferProxy);

    keyVal.close();

    Mockito.verify(mockBufferProxy, Mockito.times(2)).deallocate(Mockito.any());

    // Already closed, a no-op
    keyVal.close();

    Mockito.verify(mockBufferProxy, Mockito.times(2)).deallocate(Mockito.any());
  }
}
