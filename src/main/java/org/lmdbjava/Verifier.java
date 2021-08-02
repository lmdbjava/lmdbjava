/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2021 The LmdbJava Open Source Project
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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Verifies correct operation of LmdbJava in a given environment.
 *
 * <p>
 * Due to the large variety of operating systems and Java platforms typically
 * used with LmdbJava, this class provides a convenient verification of correct
 * operating behavior through a potentially long duration set of tests that
 * carefully verify correct storage and retrieval of successively larger
 * database entries.
 *
 * <p>
 * The verifier currently operates by incrementing a <code>long</code>
 * identifier that deterministically maps to a given {@link Dbi} and value size.
 * The key is simply the <code>long</code> identifier. The value commences with
 * a CRC that includes the identifier and the random bytes of the value. Each
 * entry is written out, and then the prior entry is retrieved using its key.
 * The prior entry's value is evaluated for accuracy and then deleted.
 * Transactions are committed in batches to ensure successive transactions
 * correctly retrieve the results of earlier transactions.
 *
 * <p>
 * Please note the verification approach may be modified in the future.
 *
 * <p>
 * If an exception is raised by this class, please:
 *
 * <ol>
 * <li>Ensure the {@link Env} passed at construction time complies with the
 * requirements specified at {@link #Verifier(org.lmdbjava.Env)}</li>
 * <li>Attempt to use a different file system to store the database (be
 * especially careful to not use network file systems, remote file systems,
 * read-only file systems etc)</li>
 * <li>Record the full exception message and stack trace, then run the verifier
 * again to see if it fails at the same or a different point</li>
 * <li>Raise a ticket on the LmdbJava Issue Tracker that confirms the above
 * details along with the failing operating system and Java version</li>
 * </ol>
 *
 */
public final class Verifier implements Callable<Long> {

  /**
   * Number of DBIs the created environment should allow.
   */
  public static final int DBI_COUNT = 5;
  private static final int BATCH_SIZE = 64;
  private static final int BUFFER_LEN = 1_024 * BATCH_SIZE;
  private static final int CRC_LENGTH = Long.BYTES;
  private static final int KEY_LENGTH = Long.BYTES;
  private final byte[] ba = new byte[BUFFER_LEN];
  private final CRC32 crc = new CRC32();
  private final List<Dbi<ByteBuffer>> dbis = new ArrayList<>(DBI_COUNT);
  private final Env<ByteBuffer> env;
  private long id;
  private final ByteBuffer key = ByteBuffer.allocateDirect(KEY_LENGTH);
  private final AtomicBoolean proceed = new AtomicBoolean(true);
  private final Random rnd = new Random();
  private Txn<ByteBuffer> txn;
  private final ByteBuffer val = ByteBuffer.allocateDirect(BUFFER_LEN);

  /**
   * Create an instance of the verifier.
   *
   * <p>
   * The caller must provide an {@link Env} configured with a suitable local
   * storage location, maximum DBIs equal to {@link #DBI_COUNT}, and a
   * map size large enough to accommodate the intended verification duration.
   *
   * <p>
   * ALL EXISTING DATA IN THE DATABASE WILL BE DELETED. The caller must not
   * interact with the <code>Env</code> in any way (eg querying, transactions
   * etc) while the verifier is executing.
   *
   * @param env target that complies with the above requirements (required)
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Verifier(final Env<ByteBuffer> env) {
    requireNonNull(env);
    this.env = env;
    key.order(BIG_ENDIAN);

    deleteDbis();
    createDbis();
  }

  /**
   * Run the verifier until {@link #stop()} is called or an exception occurs.
   *
   * <p>
   * Successful return of this method indicates no faults were detected. If any
   * fault was detected the exception message will detail the exact point that
   * the fault was encountered.
   *
   * @return number of database rows successfully verified
   */
  @Override
  public Long call() {
    try {
      while (proceed.get()) {
        transactionControl();

        write(id);

        if (id > 0) {
          fetchAndDelete(id - 1);
        }

        id++;
      }
    } finally {
      if (txn != null) {
        txn.close();
      }
    }
    return id;
  }

  /**
   * Execute the verifier for the given duration.
   *
   * <p>
   * This provides a simple way to execute the verifier for those applications
   * which do not wish to manage threads directly.
   *
   * @param duration amount of time to execute
   * @param unit     units used to express the duration
   * @return number of database rows successfully verified
   */
  public long runFor(final long duration, final TimeUnit unit) {
    final long deadline = System.currentTimeMillis() + unit.toMillis(duration);
    final ExecutorService es = Executors.newSingleThreadExecutor();
    final Future<Long> future = es.submit(this);
    try {
      while (System.currentTimeMillis() < deadline && !future.isDone()) {
        Thread.sleep(unit.toMillis(1));
      }
    } catch (final InterruptedException ignored) {
    } finally {
      stop();
    }
    final long result;
    try {
      result = future.get();
    } catch (final InterruptedException | ExecutionException ex) {
      throw new IllegalStateException(ex);
    } finally {
      es.shutdown();
    }
    return result;
  }

  private void createDbis() {
    for (int i = 0; i < DBI_COUNT; i++) {
      dbis.add(env.openDbi(Verifier.class.getSimpleName() + i, MDB_CREATE));
    }
  }

  private void deleteDbis() {
    for (final byte[] existingDbiName : env.getDbiNames()) {
      final Dbi<ByteBuffer> existingDbi = env.openDbi(existingDbiName);
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        existingDbi.drop(txn, true);
        txn.commit();
      }
    }
  }

  private void fetchAndDelete(final long forId) {
    final Dbi<ByteBuffer> dbi = getDbi(forId);
    updateKey(forId);
    final ByteBuffer fetchedValue;
    try {
      fetchedValue = dbi.get(txn, key);
    } catch (final LmdbException ex) {
      throw new IllegalStateException("DB get id=" + forId, ex);
    }

    if (fetchedValue == null) {
      throw new IllegalStateException("DB not found id=" + forId);
    }

    verifyValue(forId, fetchedValue);

    try {
      dbi.delete(txn, key);
    } catch (final LmdbException ex) {
      throw new IllegalStateException("DB del id=" + forId, ex);
    }
  }

  private Dbi<ByteBuffer> getDbi(final long forId) {
    return dbis.get((int) (forId % dbis.size()));
  }

  /**
   * Request the verifier to stop execution.
   */
  private void stop() {
    proceed.set(false);
  }

  @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
  private void transactionControl() {
    if (id % BATCH_SIZE == 0) {
      if (txn != null) {
        txn.commit();
        txn.close();
      }
      rnd.nextBytes(ba);
      txn = env.txnWrite();
    }
  }

  private void updateKey(final long forId) {
    key.clear();
    key.putLong(forId);
    key.flip();
  }

  private void updateValue(final long forId) {
    final int rndSize = valueSize(forId);
    crc.reset();
    crc.update((int) forId);
    crc.update(ba, CRC_LENGTH, rndSize);
    final long crcVal = crc.getValue();

    val.clear();
    val.putLong(crcVal);
    val.put(ba, CRC_LENGTH, rndSize);
    val.flip();
  }

  private int valueSize(final long forId) {
    final int mod = (int) (forId % BATCH_SIZE);
    final int base = 1_024 * mod;
    final int value = base == 0 ? 512 : base;
    return value - CRC_LENGTH - KEY_LENGTH; // aim to minimise partial pages
  }

  private void verifyValue(final long forId, final ByteBuffer bb) {
    final int rndSize = valueSize(forId);
    final int expected = rndSize + CRC_LENGTH;
    if (bb.limit() != expected) {
      throw new IllegalStateException("Limit error id=" + forId + " exp="
                                          + expected + " limit=" + bb.limit());
    }

    final long crcRead = bb.getLong();
    crc.reset();
    crc.update((int) forId);
    crc.update(bb);
    final long crcVal = crc.getValue();

    if (crcRead != crcVal) {
      throw new IllegalStateException("CRC error id=" + forId);
    }
  }

  private void write(final long forId) {
    final Dbi<ByteBuffer> dbi = getDbi(forId);
    updateKey(forId);
    updateValue(forId);

    try {
      dbi.put(txn, key, val);
    } catch (final LmdbException ex) {
      throw new IllegalStateException("DB put id=" + forId, ex);
    }
  }

}
