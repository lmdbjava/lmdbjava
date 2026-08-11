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
