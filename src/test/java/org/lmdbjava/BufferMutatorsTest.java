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

import static java.lang.Class.forName;
import static java.lang.Integer.BYTES;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Test;
import static org.lmdbjava.BufferMutators.FIELD_NAME_ADDRESS;
import static org.lmdbjava.BufferMutators.MUTATOR;
import static org.lmdbjava.BufferMutators.NAME_REFLECTIVE;
import static org.lmdbjava.BufferMutators.NAME_UNSAFE;
import static org.lmdbjava.BufferMutators.SUPPORTS_UNSAFE;
import static org.lmdbjava.BufferMutators.findField;
import static org.lmdbjava.BufferMutators.requireDirectBuffer;
import org.lmdbjava.LmdbException.BufferNotDirectException;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

public class BufferMutatorsTest {

  private static final Field FIELD_ADDRESS;

  static {
    try {
      FIELD_ADDRESS = findField(Buffer.class, FIELD_NAME_ADDRESS);
    } catch (NoSuchFieldException ex) {
      throw new RuntimeException(ex);
    }
  }

  private long address1;
  private long address2;
  private ByteBuffer buffer1;
  private ByteBuffer buffer2;

  @Before
  public void before() throws Exception {
    buffer1 = allocateDirect(BYTES);
    buffer2 = allocateDirect(BYTES * 2);

    buffer1.putInt(0, MIN_VALUE);
    buffer2.putInt(0, MIN_VALUE + 1);
    buffer2.putInt(BYTES, MAX_VALUE - 1);

    address1 = FIELD_ADDRESS.getLong(buffer1);
    address2 = FIELD_ADDRESS.getLong(buffer2);
  }

  @Test
  public void coverPrivateConstructors() throws Exception {
    invokePrivateConstructor(BufferMutators.class);
  }

  @Test
  public void directBufferAllowed() throws Exception {
    requireDirectBuffer(allocateDirect(BYTES));
  }

  @Test
  public void instance() throws Exception {
    final BufferMutator bm = construct(NAME_REFLECTIVE);
    assertThat(bm, is(notNullValue()));
    swapTest(MUTATOR);
  }

  @Test(expected = BufferNotDirectException.class)
  public void javaBufferRejected() throws Exception {
    requireDirectBuffer(allocate(BYTES));
  }

  @Test
  public void reflector() throws Exception {
    final BufferMutator bm = construct(NAME_REFLECTIVE);
    assertThat(bm, is(notNullValue()));
    swapTest(bm);
  }

  @Test
  public void unsafe() throws Exception {
    if (!SUPPORTS_UNSAFE) {
      return;
    }
    final BufferMutator bm = construct(NAME_UNSAFE);
    assertThat(bm, is(notNullValue()));
    swapTest(bm);
  }

  private BufferMutator construct(final String className) throws
      ClassNotFoundException,
      NoSuchMethodException,
      InstantiationException,
      IllegalAccessException,
      IllegalArgumentException,
      InvocationTargetException {
    Class<?> clazz = forName(className);
    Constructor<?> c = clazz.getDeclaredConstructor();
    c.setAccessible(true);
    return (BufferMutator) c.newInstance();
  }

  private void swapTest(BufferMutator bm) {
    // as per before()
    assertThat(buffer1.capacity(), is(BYTES));
    assertThat(buffer1.getInt(0), is(MIN_VALUE));
    assertThat(buffer2.capacity(), is(BYTES * 2));
    assertThat(buffer2.getInt(0), is(MIN_VALUE + 1));
    assertThat(buffer2.getInt(BYTES), is(MAX_VALUE - 1));

    // make buffer2 identical to buffer1
    bm.modify(buffer2, address1, BYTES);
    assertThat(buffer1.capacity(), is(BYTES));
    assertThat(buffer1.getInt(0), is(MIN_VALUE));
    assertThat(buffer2.capacity(), is(BYTES));
    assertThat(buffer2.getInt(0), is(MIN_VALUE));

    // make buffer1 hold what buffer2 used to
    bm.modify(buffer1, address2, BYTES * 2);
    assertThat(buffer1.capacity(), is(BYTES * 2));
    assertThat(buffer1.getInt(0), is(MIN_VALUE + 1));
    assertThat(buffer1.getInt(BYTES), is(MAX_VALUE - 1));
    assertThat(buffer2.capacity(), is(BYTES));
    assertThat(buffer2.getInt(0), is(MIN_VALUE));

    // back to the original layout
    bm.modify(buffer1, address1, BYTES);
    bm.modify(buffer2, address2, BYTES * 2);

    // as per before()
    assertThat(buffer1.capacity(), is(BYTES));
    assertThat(buffer1.getInt(0), is(MIN_VALUE));
    assertThat(buffer2.capacity(), is(BYTES * 2));
    assertThat(buffer2.getInt(0), is(MIN_VALUE + 1));
    assertThat(buffer2.getInt(BYTES), is(MAX_VALUE - 1));
  }
}
