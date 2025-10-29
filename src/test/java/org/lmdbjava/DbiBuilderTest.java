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

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.bb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DbiBuilderTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<ByteBuffer> env;

  @After
  public void after() {
    env.close();
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = create()
        .setMapSize(MEBIBYTES.toBytes(64))
        .setMaxReaders(2)
        .setMaxDbs(2)
        .open(path, MDB_NOSUBDIR);
  }

  @Test
  public void unnamed() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .withoutDbName()
        .withDefaultComparator()
        .withDbiFlags(DbiFlags.MDB_CREATE)
        .open();

    assertThat(env.getDbiNames().size(), Matchers.is(0));

    assertPutAndGet(dbi);
  }


  @Test
  public void named() {
    final Dbi<ByteBuffer> dbi = env.buildDbi()
        .withDbName("foo")
        .withDefaultComparator()
        .withDbiFlags(DbiFlags.MDB_CREATE)
        .open();

    assertPutAndGet(dbi);

    assertThat(env.getDbiNames().size(), Matchers.is(1));
    assertThat(new String(env.getDbiNames().get(0), StandardCharsets.UTF_8), Matchers.is("foo"));
  }

  private void assertPutAndGet(Dbi<ByteBuffer> dbi) {
    try (Txn<ByteBuffer> writeTxn = env.txnWrite()) {
      dbi.put(writeTxn, bb(123), bb(123_000));
      writeTxn.commit();
    }

    try (Txn<ByteBuffer> readTxn = env.txnRead()) {
      final ByteBuffer byteBuffer = dbi.get(readTxn, bb(123));
      final int val = byteBuffer.getInt();
      assertThat(val, Matchers.is(123_000));
    }
  }
}
