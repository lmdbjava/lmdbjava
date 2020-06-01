/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
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

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Dbi.BadValueSizeException;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env.AlreadyClosedException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.KeyRange.closed;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;
import org.lmdbjava.Txn.EnvIsReadOnly;
import org.lmdbjava.Txn.IncompatibleParent;
import org.lmdbjava.Txn.NotReadyException;
import org.lmdbjava.Txn.NotResetException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import org.lmdbjava.Txn.ReadWriteRequiredException;
import org.lmdbjava.Txn.ResetException;
import static org.lmdbjava.Txn.State.DONE;
import static org.lmdbjava.Txn.State.READY;
import static org.lmdbjava.Txn.State.RELEASED;
import static org.lmdbjava.Txn.State.RESET;
import static org.lmdbjava.TxnFlags.MDB_RDONLY_TXN;

/**
 * Test {@link Txn}.
 */
public final class TxnTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<ByteBuffer> env;
  private File path;

  @After
  public void after() {
    env.close();
  }

  @Before
  public void before() throws IOException {
    path = tmp.newFile();
    env = create()
        .setMapSize(KIBIBYTES.toBytes(100))
        .setMaxReaders(1)
        .setMaxDbs(2)
        .open(path, POSIX_MODE, MDB_NOSUBDIR);
  }

  @Test(expected = BadValueSizeException.class)
  public void largeKeysRejected() throws IOException {
    final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE);
    final ByteBuffer key = allocateDirect(env.getMaxKeySize() + 1);
    key.limit(key.capacity());
    dbi.put(key, bb(2));
  }

  @Test
  public void rangeSearch() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final ByteBuffer key = allocateDirect(env.getMaxKeySize());
    key.put("cherry".getBytes(UTF_8)).flip();
    db.put(key, bb(1));

    key.clear();
    key.put("strawberry".getBytes(UTF_8)).flip();
    db.put(key, bb(3));

    key.clear();
    key.put("pineapple".getBytes(UTF_8)).flip();
    db.put(key, bb(2));

    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer start = allocateDirect(env.getMaxKeySize());
      start.put("a".getBytes(UTF_8)).flip();

      final ByteBuffer end = allocateDirect(env.getMaxKeySize());
      end.put("z".getBytes(UTF_8)).flip();

      final List<String> keysFound = new ArrayList<>();
      try (CursorIterable<ByteBuffer> ckr = db.iterate(txn, closed(start, end))) {
        for (final CursorIterable.KeyVal<ByteBuffer> kv : ckr) {
          keysFound.add(UTF_8.decode(kv.key()).toString());
        }
      }

      assertEquals(3, keysFound.size());

    }
  }

  @Test
  public void readOnlyTxnAllowedInReadOnlyEnv() {
    env.openDbi(DB_1, MDB_CREATE);
    try (Env<ByteBuffer> roEnv = create()
        .setMaxReaders(1)
        .open(path, MDB_NOSUBDIR, MDB_RDONLY_ENV)) {
      assertThat(roEnv.txnRead(), is(notNullValue()));
    }
  }

  @Test(expected = EnvIsReadOnly.class)
  public void readWriteTxnDeniedInReadOnlyEnv() {
    env.openDbi(DB_1, MDB_CREATE);
    env.close();
    try (Env<ByteBuffer> roEnv = create()
        .setMaxReaders(1)
        .open(path, MDB_NOSUBDIR, MDB_RDONLY_ENV)) {
      roEnv.txnWrite(); // error
    }
  }

  @Test(expected = NotReadyException.class)
  public void testCheckNotCommitted() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      txn.commit();
      txn.checkReady();
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void testCheckReadOnly() {
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      txn.checkReadOnly();
    }
  }

  @Test(expected = ReadWriteRequiredException.class)
  public void testCheckWritesAllowed() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      txn.checkWritesAllowed();
    }
  }

  @Test
  public void testGetId() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final AtomicLong txId1 = new AtomicLong();
    final AtomicLong txId2 = new AtomicLong();

    try (Txn<ByteBuffer> tx1 = env.txnRead()) {
      txId1.set(tx1.getId());
    }

    db.put(bb(1), bb(2));

    try (Txn<ByteBuffer> tx2 = env.txnRead()) {
      txId2.set(tx2.getId());
    }
    // should not see the same snapshot
    assertThat(txId1.get(), is(not(txId2.get())));
  }

  @Test
  public void txCanCommitThenCloseWithoutError() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.getState(), is(READY));
      txn.commit();
      assertThat(txn.getState(), is(DONE));
    }
  }

  @Test(expected = NotReadyException.class)
  public void txCannotAbortIfAlreadyCommitted() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.getState(), is(READY));
      txn.commit();
      assertThat(txn.getState(), is(DONE));
      txn.abort();
    }
  }

  @Test(expected = NotReadyException.class)
  public void txCannotCommitTwice() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      txn.commit();
      txn.commit(); // error
    }
  }

  @Test(expected = AlreadyClosedException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void txConstructionDeniedIfEnvClosed() {
    env.close();
    env.txnRead();
  }

  @Test
  public void txParent() {
    try (Txn<ByteBuffer> txRoot = env.txnWrite();
         Txn<ByteBuffer> txChild = env.txn(txRoot)) {
      assertThat(txRoot.getParent(), is(nullValue()));
      assertThat(txChild.getParent(), is(txRoot));
    }
  }

  @Test(expected = IncompatibleParent.class)
  public void txParentROChildRWIncompatible() {
    try (Txn<ByteBuffer> txRoot = env.txnRead()) {
      env.txn(txRoot); // error
    }
  }

  @Test(expected = IncompatibleParent.class)
  public void txParentRWChildROIncompatible() {
    try (Txn<ByteBuffer> txRoot = env.txnWrite()) {
      env.txn(txRoot, MDB_RDONLY_TXN); // error
    }
  }

  @Test
  public void txReadOnly() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.getParent(), is(nullValue()));
      assertThat(txn.getState(), is(READY));
      assertThat(txn.isReadOnly(), is(true));
      txn.checkReady();
      txn.checkReadOnly();
      txn.reset();
      assertThat(txn.getState(), is(RESET));
      txn.renew();
      assertThat(txn.getState(), is(READY));
      txn.commit();
      assertThat(txn.getState(), is(DONE));
      txn.close();
      assertThat(txn.getState(), is(RELEASED));
    }
  }

  @Test
  public void txReadWrite() {
    final Txn<ByteBuffer> txn = env.txnWrite();
    assertThat(txn.getParent(), is(nullValue()));
    assertThat(txn.getState(), is(READY));
    assertThat(txn.isReadOnly(), is(false));
    txn.checkReady();
    txn.checkWritesAllowed();
    txn.commit();
    assertThat(txn.getState(), is(DONE));
    txn.close();
    assertThat(txn.getState(), is(RELEASED));
  }

  @Test(expected = NotResetException.class)
  public void txRenewDeniedWithoutPriorReset() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      txn.renew();
    }
  }

  @Test(expected = ResetException.class)
  public void txResetDeniedForAlreadyResetTransaction() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      txn.reset();
      txn.renew();
      txn.reset();
      txn.reset();
    }
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void txResetDeniedForReadWriteTransaction() {
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      txn.reset();
    }
  }

  @Test(expected = BadValueSizeException.class)
  public void zeroByteKeysRejected() throws IOException {
    final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE);
    final ByteBuffer key = allocateDirect(4);
    key.putInt(1);
    assertThat(key.remaining(), is(0)); // because key.flip() skipped
    dbi.put(key, bb(2));
  }

}
