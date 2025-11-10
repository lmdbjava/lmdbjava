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

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_APPEND;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.bb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CursorIterablePerfTest {

  private static final int ITERATIONS = 100_000;

  private TempDir tempDir;
  private final List<Dbi<ByteBuffer>> dbs = new ArrayList<>();
  private final List<Integer> data = new ArrayList<>(ITERATIONS);
  private Env<ByteBuffer> env;

  @BeforeEach
  public void before() throws IOException {
    tempDir = new TempDir();
    final BufferProxy<ByteBuffer> bufferProxy = ByteBufferProxy.PROXY_OPTIMAL;
    env =
        create(bufferProxy)
            .setMapSize(1, ByteUnit.GIBIBYTES)
            .setMaxReaders(1)
            .setMaxDbs(3)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(tempDir.createTempFile());

    final DbiFlagSet dbiFlagSet = MDB_CREATE;
    // Use a java comparator for start/stop keys only
    Dbi<ByteBuffer> dbJavaComparator =
        env.createDbi()
            .setDbName("JavaComparator")
            .withDefaultComparator()
            .setDbiFlags(dbiFlagSet)
            .open();
    // Use LMDB comparator for start/stop keys
    Dbi<ByteBuffer> dbLmdbComparator =
        env.createDbi()
            .setDbName("LmdbComparator")
            .withNativeComparator()
            .setDbiFlags(dbiFlagSet)
            .open();

    // Use a java comparator for start/stop keys and as a callback comparator
    Dbi<ByteBuffer> dbCallbackComparator =
        env.createDbi()
            .setDbName("CallBackComparator")
            .withCallbackComparator(bufferProxy::getComparator)
            .setDbiFlags(dbiFlagSet)
            .open();

    dbs.add(dbJavaComparator);
    dbs.add(dbLmdbComparator);
    dbs.add(dbCallbackComparator);

    populateList();
  }

  @AfterEach
  public void after() {
    env.close();
    tempDir.cleanup();
  }

  private void populateList() {
    for (int i = 0; i < ITERATIONS * 2; i += 2) {
      data.add(i);
    }
  }

  private void populateDatabases(final boolean randomOrder) {
    System.out.println("Clear then populate databases (randomOrder=" + randomOrder + ")");

    final List<Integer> data;
    if (randomOrder) {
      data = new ArrayList<>(this.data);
      Collections.shuffle(data);
    } else {
      data = this.data;
    }

    final PutFlagSet noOverwriteAndAppendFlagSet = PutFlagSet.of(MDB_NOOVERWRITE, MDB_APPEND);

    for (int round = 0; round < 3; round++) {
      System.out.println("round: " + round + " -----------------------------------------");

      for (final Dbi<ByteBuffer> db : dbs) {
        // Clean out the db first
        try (Txn<ByteBuffer> txn = env.txnWrite();
            final Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
          while (cursor.next()) {
            cursor.delete();
          }
        }

        final String dbName = db.getNameAsString(StandardCharsets.UTF_8);
        final Instant start = Instant.now();
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
          for (final Integer i : data) {
            if (randomOrder) {
              db.put(txn, bb(i), bb(i + 1), MDB_NOOVERWRITE);
            } else {
              db.put(txn, bb(i), bb(i + 1), noOverwriteAndAppendFlagSet);
            }
          }
          txn.commit();
        }
        final Duration duration = Duration.between(start, Instant.now());
        System.out.println(
            "DB: "
                + dbName
                + " - Loaded in duration: "
                + duration
                + ", millis: "
                + duration.toMillis());
      }
    }
  }

  @Test
  public void comparePerf_sequential() {
    comparePerf(false);
  }

  @Test
  public void comparePerf_random() {
    comparePerf(true);
  }

  public void comparePerf(final boolean randomOrder) {
    populateDatabases(randomOrder);
    final ByteBuffer startKeyBuf = bb(data.get(0));
    final ByteBuffer stopKeyBuf = bb(data.get(data.size() - 1));
    final KeyRange<ByteBuffer> keyRange = KeyRange.closed(startKeyBuf, stopKeyBuf);

    System.out.println("\nIterating over all entries");
    for (int round = 0; round < 3; round++) {
      System.out.println("round: " + round + " -----------------------------------------");
      for (final Dbi<ByteBuffer> db : dbs) {
        final String dbName = db.getNameAsString();

        final Instant start = Instant.now();
        int cnt = 0;
        // Exercise the stop key comparator on every entry
        try (Txn<ByteBuffer> txn = env.txnRead();
            CursorIterable<ByteBuffer> cursorIterable = db.iterate(txn, keyRange)) {
          for (final CursorIterable.KeyVal<ByteBuffer> ignored : cursorIterable) {
            cnt++;
          }
        }
        final Duration duration = Duration.between(start, Instant.now());
        System.out.println(
            "DB: "
                + dbName
                + " - Iterated in duration: "
                + duration
                + ", millis: "
                + duration.toMillis()
                + ", cnt: "
                + cnt);
      }
    }
  }
}
