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
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import jnr.ffi.Pointer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;
import static org.lmdbjava.ValueBuffers.allocateMdbVal;
import static org.lmdbjava.ValueBuffers.setBufferToPointer;
import static org.lmdbjava.ValueBuffers.setPointerToBuffer;

public class ValueBuffersTest {

  private ByteBuffer b1In;
  private ByteBuffer b1Out;
  private ByteBuffer b2In;
  private ByteBuffer b2Out;
  private ByteBuffer b3In;
  private ByteBuffer b3Out;

  @Test
  public void allocateDirectFromBuffer() throws BufferNotDirectException {
    final Pointer p1 = allocateMdbVal(b1In);
    final Pointer p2 = allocateMdbVal(b2In);
    final Pointer p3 = allocateMdbVal(b3In);
    final Pointer p4 = allocateMdbVal(null);

    assertThat(p1, not(nullValue()));
    assertThat(p2, not(nullValue()));
    assertThat(p3, not(nullValue()));
    assertThat(p4, is(nullValue()));

    setBufferToPointer(p1, b1Out);
    setBufferToPointer(p2, b2Out);
    setBufferToPointer(p3, b3Out);

    checkOut();
  }

  @Before
  public void before() throws Exception {
    b1In = allocateDirect(BYTES);
    b2In = allocateDirect(BYTES * 2);
    b3In = allocateDirect(BYTES * 3);

    b1Out = allocateDirect(BYTES);
    b2Out = allocateDirect(BYTES * 2);
    b3Out = allocateDirect(BYTES * 3);

    b1In.putInt(0, 1);
    b2In.putInt(0, 2);
    b2In.putInt(BYTES, 3);
    b3In.putInt(0, 4);
    b3In.putInt(BYTES, 5);
    b3In.putInt(BYTES * 2, 6);

    checkIn();
  }

  @Test
  public void coverPrivateConstructors() throws Exception {
    invokePrivateConstructor(ValueBuffers.class);
  }

  @Test
  public void inCanPointAtSelfWithoutChange() throws BufferNotDirectException {
    final Pointer p1 = allocateMdbVal();
    final Pointer p2 = allocateMdbVal();
    final Pointer p3 = allocateMdbVal();

    assertThat(p1, not(nullValue()));
    assertThat(p2, not(nullValue()));
    assertThat(p3, not(nullValue()));

    setPointerToBuffer(b1In, p1);
    setPointerToBuffer(b2In, p2);
    setPointerToBuffer(b3In, p3);

    setBufferToPointer(p1, b1In);
    setBufferToPointer(p2, b2In);
    setBufferToPointer(p3, b3In);

    checkIn();
  }

  @Test
  public void inToOutViaPointer() throws BufferNotDirectException {
    final Pointer p1 = allocateMdbVal();
    final Pointer p2 = allocateMdbVal();
    final Pointer p3 = allocateMdbVal();

    assertThat(p1, not(nullValue()));
    assertThat(p2, not(nullValue()));
    assertThat(p3, not(nullValue()));

    setPointerToBuffer(b1In, p1);
    setPointerToBuffer(b2In, p2);
    setPointerToBuffer(b3In, p3);

    setBufferToPointer(p1, b1Out);
    setBufferToPointer(p2, b2Out);
    setBufferToPointer(p3, b3Out);

    checkOut();
  }

  private void checkIn() {
    assertThat(b1In.capacity(), is(BYTES));
    assertThat(b2In.capacity(), is(BYTES * 2));
    assertThat(b3In.capacity(), is(BYTES * 3));
    assertThat(b1In.getInt(0), is(1));
    assertThat(b2In.getInt(0), is(2));
    assertThat(b2In.getInt(BYTES), is(3));
    assertThat(b3In.getInt(0), is(4));
    assertThat(b3In.getInt(BYTES), is(5));
    assertThat(b3In.getInt(BYTES * 2), is(6));
  }

  private void checkOut() {
    assertThat(b1Out.capacity(), is(BYTES));
    assertThat(b2Out.capacity(), is(BYTES * 2));
    assertThat(b3Out.capacity(), is(BYTES * 3));
    assertThat(b1Out.getInt(0), is(1));
    assertThat(b2Out.getInt(0), is(2));
    assertThat(b2Out.getInt(BYTES), is(3));
    assertThat(b3Out.getInt(0), is(4));
    assertThat(b3Out.getInt(BYTES), is(5));
    assertThat(b3Out.getInt(BYTES * 2), is(6));
  }

}
