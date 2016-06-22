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

import java.nio.ByteBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import org.lmdbjava.BufferProxy.BufferProxyFactory;
import static org.lmdbjava.ByteBufferProxy.FACTORY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.FACTORY_SAFE;
import org.lmdbjava.ByteBufferProxy.ReflectiveProxyFactory;
import org.lmdbjava.ByteBufferProxy.UnsafeProxyFactory;
import static org.lmdbjava.ByteBufferProxy.factory;
import static org.lmdbjava.ByteBufferProxy.findField;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;
import static org.lmdbjava.UnsafeAccess.ALLOW_UNSAFE;

public class ByteBufferProxyTest {

  private static final String REFLECT = ReflectiveProxyFactory.class.getName();
  private static final String UNSAFE = UnsafeProxyFactory.class.getName();

  @Test
  public void coverPrivateConstructor() throws Exception {
    invokePrivateConstructor(ByteBufferProxy.class);
  }

  @Test
  public void coverageOnly() {
    assertThat(factory("not a class"), is(nullValue()));
  }

  @Test(expected = RuntimeException.class)
  public void missingFieldRaisesException() throws Exception {
    findField(Long.class, "notARealField");
  }

  @Test
  public void safeCanBeForced() throws Exception {
    final BufferProxyFactory<ByteBuffer> v = FACTORY_SAFE;
    assertThat(v, is(notNullValue()));
    assertThat(v.getClass().getName(), is(REFLECT));
  }

  @Test
  public void unsafeIsDefault() throws Exception {
    assertThat(ALLOW_UNSAFE, is(true));
    final BufferProxyFactory<ByteBuffer> v = FACTORY_OPTIMAL;
    assertThat(v, is(notNullValue()));
    assertThat(v.getClass().getName(), is(UNSAFE));
  }

}
