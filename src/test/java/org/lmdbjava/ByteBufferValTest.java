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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import static org.lmdbjava.ByteBufferVal.requireDirectBuffer;
import org.lmdbjava.ByteBufferVals.ReflectiveByteBufferVal;
import org.lmdbjava.ByteBufferVals.UnsafeByteBufferVal;
import static org.lmdbjava.ByteBufferVals.factory;
import static org.lmdbjava.ByteBufferVals.findField;
import static org.lmdbjava.ByteBufferVals.forBuffer;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

public class ByteBufferValTest {

  private static final String REFLECT = ReflectiveByteBufferVal.class.getName();
  private static final String UNSAFE = UnsafeByteBufferVal.class.getName();

  @Test
  public void coverPrivateConstructor() throws Exception {
    invokePrivateConstructor(ByteBufferVals.class);
  }

  @Test
  public void coverageOnly() {
    assertThat(factory("not a class"), is(nullValue()));
  }

  @Test
  public void directBufferAllowed() throws Exception {
    requireDirectBuffer(allocateDirect(BYTES));
  }

  @Test(expected = BufferNotDirectException.class)
  public void javaBufferRejected() throws Exception {
    requireDirectBuffer(allocate(BYTES));
  }

  @Test(expected = RuntimeException.class)
  public void missingFieldRaisesException() throws Exception {
    findField(UnsafeByteBufferVal.class, "notARealField");
  }

  @Test
  public void safeCanBeForced() throws Exception {
    final ByteBufferVal v = forBuffer(allocateDirect(BYTES), true, true);
    assertThat(v, is(notNullValue()));
    assertThat(v.getClass().getName(), is(REFLECT));
  }

  @Test
  public void unsafeCanBeForced() throws Exception {
    final ByteBufferVal v = forBuffer(allocateDirect(BYTES), true, false);
    assertThat(v, is(notNullValue()));
    assertThat(v.getClass().getName(), is(UnsafeByteBufferVal.class.getName()));
  }

  @Test
  public void unsafeIsDefault() throws Exception {
    final ByteBufferVal v = forBuffer(allocateDirect(BYTES));
    assertThat(v, is(notNullValue()));
    assertThat(v.getClass().getName(), is(UNSAFE));
  }

}
