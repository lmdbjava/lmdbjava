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

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class GarbageCollectionTest {

  private static final String DB_NAME = "my DB";
  private static final String KEY_PREFIX = "Uncorruptedkey";
  private static final String VAL_PREFIX = "Uncorruptedval";

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void buffersNotGarbageCollectedTest() throws IOException {
    final File path = tmp.newFolder();
    try (Env<ByteBuffer> env = create().setMapSize(2_085_760_999).setMaxDbs(1).open(path)) {
      final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        for (int i = 0; i < 5_000; i++) {
          putBuffer(db, txn, i);
        }
        txn.commit();
      }

      // Call GC before writing to LMDB and after last reference to buffer by
      // changing the behavior of mask
      try (MockedStatic<MaskedFlag> mockedStatic = Mockito.mockStatic(MaskedFlag.class)) {
        mockedStatic
            .when(MaskedFlag::mask)
            .thenAnswer(
                invocationOnMock -> {
                  System.gc();
                  return 0;
                });
        final int gcRecordWrites = Integer.getInteger("gcRecordWrites", 50);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
          for (int i = 0; i < gcRecordWrites; i++) {
            putBuffer(db, txn, i);
          }
          txn.commit();
        }
      }

      // Find corrupt keys
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
          if (c.first()) {
            do {
              final byte[] rkey = new byte[c.key().remaining()];
              c.key().get(rkey);
              final byte[] rval = new byte[c.val().remaining()];
              c.val().get(rval);
              final String skey = new String(rkey, UTF_8);
              final String sval = new String(rval, UTF_8);
              if (!skey.startsWith("Uncorruptedkey")) {
                fail("Found corrupt key " + skey);
              }
              if (!sval.startsWith("Uncorruptedval")) {
                fail("Found corrupt val " + sval);
              }
            } while (c.next());
          }
        }
      }
    }
  }

  private void putBuffer(final Dbi<ByteBuffer> db, final Txn<ByteBuffer> txn, final int i) {
    final ByteBuffer key = allocateDirect(24);
    final ByteBuffer val = allocateDirect(24);
    key.put((KEY_PREFIX + i).getBytes(UTF_8)).flip();
    val.put((VAL_PREFIX + i).getBytes(UTF_8)).flip();
    db.put(txn, key, val);
  }
}
