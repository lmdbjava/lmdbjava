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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Env.CloseTimeoutException;

/**
 * Tests the opt-in "safe close" ({@link Env.Builder#setSafeClose(boolean)} + {@link
 * Env#close(Duration)}). These deterministically exercise the close-during-read hazard that,
 * without safe close, is undefined behaviour crashing the JVM in {@code mdb_txn_renew0} (see
 * lmdbjava#253). With safe close enabled the same scenarios are safe, so they run green in the
 * normal suite.
 */
public final class EnvSafeCloseTest {

  private TempDir tempDir;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
  }

  @AfterEach
  void afterEach() {
    tempDir.cleanup();
  }

  private Env<ByteBuffer> openSafe() {
    final Path dir = tempDir.createTempDir();
    return Env.create().setMaxReaders(64).setMaxDbs(1).setSafeClose(true).open(dir);
  }

  @Test
  void isSafeClose_reflectsBuilder() {
    try (Env<ByteBuffer> safe = openSafe()) {
      assertThat(safe.isSafeClose()).isTrue();
    }
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> plain = Env.create().setMaxDbs(1).open(dir)) {
      assertThat(plain.isSafeClose()).isFalse();
    }
  }

  @Test
  void closeDuration_withoutSafeClose_throwsIllegalState() {
    final Path dir = tempDir.createTempDir();
    try (Env<ByteBuffer> plain = Env.create().setMaxDbs(1).open(dir)) {
      assertThatThrownBy(() -> plain.close(Duration.ofSeconds(1)))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void closeDuration_withOpenTxn_timesOutAndDoesNotClose() {
    final Env<ByteBuffer> env = openSafe();
    env.createDbi().setDbName(DB_1).withDefaultComparator().addDbiFlag(MDB_CREATE).open();

    final Txn<ByteBuffer> reader = env.txnRead(); // deliberately left open
    assertThatThrownBy(() -> env.close(Duration.ofMillis(200)))
        .isInstanceOf(CloseTimeoutException.class);
    assertThat(env.isClosed()).isFalse(); // must NOT have unmapped with a live reader

    reader.close();
    env.close(Duration.ofSeconds(5)); // now drains cleanly
    assertThat(env.isClosed()).isTrue();
  }

  @Test
  void closeDuration_blocksUntilReaderCloses_thenRejectsNewTxns() throws Exception {
    final Env<ByteBuffer> env = openSafe();
    env.createDbi().setDbName(DB_1).withDefaultComparator().addDbiFlag(MDB_CREATE).open();

    final Txn<ByteBuffer> reader = env.txnRead();
    final CompletableFuture<Void> closeFuture =
        CompletableFuture.runAsync(() -> env.close(Duration.ofSeconds(5)));

    Thread.sleep(150);
    assertThat(closeFuture).isNotDone(); // blocked on the live reader
    assertThat(env.isClosed()).isFalse();

    reader.close();
    closeFuture.get(5, TimeUnit.SECONDS); // completes once the reader drained
    assertThat(env.isClosed()).isTrue();
    assertThatThrownBy(env::txnRead).isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void closeDuration_isIdempotent() {
    final Env<ByteBuffer> env = openSafe();
    env.close(Duration.ofSeconds(1));
    env.close(Duration.ofSeconds(1)); // no-op second call
    assertThat(env.isClosed()).isTrue();
  }

  /**
   * The core regression: hammer reads from many threads, then safe-close while reads are in flight.
   * Without safe close this crashes the JVM; with it the close drains in-flight readers, later
   * reads observe {@link AlreadyClosedException}, and the JVM survives.
   */
  @Test
  void closeDuringConcurrentReads_survivesAndClosesCleanly() throws Exception {
    final Env<ByteBuffer> env = openSafe();
    final Dbi<ByteBuffer> db =
        env.createDbi().setDbName(DB_1).withDefaultComparator().addDbiFlag(MDB_CREATE).open();
    for (int i = 0; i < 32; i++) {
      db.put(bb(i), bb(i));
    }

    final int readerCount = 16;
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong reads = new AtomicLong();
    final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    final List<Thread> readers = new ArrayList<>(readerCount);

    for (int i = 0; i < readerCount; i++) {
      final int seed = i;
      final Thread reader =
          new Thread(
              () -> {
                int k = seed;
                while (!stop.get()) {
                  try (Txn<ByteBuffer> txn = env.txnRead()) {
                    db.get(txn, bb(k & 31));
                    reads.incrementAndGet();
                    k++;
                  } catch (final AlreadyClosedException expected) {
                    return; // benign: env is closing/closed
                  } catch (final Throwable t) {
                    unexpected.add(t);
                    return;
                  }
                }
              },
              "reader-" + seed);
      reader.setDaemon(true);
      reader.start();
      readers.add(reader);
    }

    Thread.sleep(100); // let readers saturate the native read path
    env.close(Duration.ofSeconds(10)); // THE RACE, made safe by drain
    stop.set(true);
    for (final Thread reader : readers) {
      reader.join(5_000);
    }

    assertThat(reads.get()).isGreaterThan(0L);
    assertThat(unexpected).isEmpty();
    assertThat(env.isClosed()).isTrue();
  }
}
