/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
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

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test case for #17.
 */
public final class Issue17Test {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  
  @Test
  public void test() {
  
    final Env<ByteBuffer> env = Env.create()
        .setMapSize(MEBIBYTES.toBytes(8))
        .setMaxDbs(1)
        .open(temp.getRoot());
    final Dbi<ByteBuffer> db = env.openDbi("test", DbiFlags.MDB_CREATE);

    try {
      final byte[] k = new byte[500];
      final ByteBuffer key = ByteBuffer.allocateDirect(500);
      final ByteBuffer val = ByteBuffer.allocateDirect(1024);

      final ThreadLocalRandom rnd = ThreadLocalRandom.current();
      int count = 0;
      for (;;) {
        rnd.nextBytes(k);
        key.clear();
        key.put(k).flip();
        val.clear();
        db.put(key, val);
        System.out.println("written " + ++count);
      }
    } catch (final Env.MapFullException e) {
      // expected
    }

    System.out.println("closing db");
    db.close();

    System.out.println("closing env");
    env.close();
  }

}
