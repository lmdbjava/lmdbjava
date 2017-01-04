/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
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
import java.util.concurrent.atomic.AtomicLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.After;
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
  public void readOnlyTxnAllowedInReadOnlyEnv() {
    env.openDbi(DB_1, MDB_CREATE);
    final Env<ByteBuffer> roEnv = create().open(path, MDB_NOSUBDIR,
                                                MDB_RDONLY_ENV);
    assertThat(roEnv.txnRead(), is(notNullValue()));
  }

  @Test(expected = EnvIsReadOnly.class)
  public void readWriteTxnDeniedInReadOnlyEnv() {
    env.openDbi(DB_1, MDB_CREATE);
    env.close();
    final Env<ByteBuffer> roEnv = create().open(path, MDB_NOSUBDIR,
                                                MDB_RDONLY_ENV);
    roEnv.txnWrite(); // error
  }

  @Test(expected = NotReadyException.class)
  public void testCheckNotCommitted() {
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.commit();
    txn.checkReady();
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void testCheckReadOnly() {
    final Txn<ByteBuffer> txn = env.txnWrite();
    txn.checkReadOnly();
  }

  @Test(expected = ReadWriteRequiredException.class)
  public void testCheckWritesAllowed() {
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.checkWritesAllowed();
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
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.commit();
    txn.commit(); // error
  }

  @Test(expected = AlreadyClosedException.class)
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void txConstructionDeniedIfEnvClosed() {
    env.close();
    env.txnRead();
  }

  @Test
  public void txParent() {
    final Txn<ByteBuffer> txRoot = env.txnWrite();
    final Txn<ByteBuffer> txChild = env.txn(txRoot);
    assertThat(txRoot.getParent(), is(nullValue()));
    assertThat(txChild.getParent(), is(txRoot));
  }

  @Test(expected = IncompatibleParent.class)
  public void txParentROChildRWIncompatible() {
    final Txn<ByteBuffer> txRoot = env.txnRead();
    env.txn(txRoot); // error
  }

  @Test(expected = IncompatibleParent.class)
  public void txParentRWChildROIncompatible() {
    final Txn<ByteBuffer> txRoot = env.txnWrite();
    env.txn(txRoot, MDB_RDONLY_TXN); // error
  }

  @Test
  public void txReadOnly() {
    final Txn<ByteBuffer> txn = env.txnRead();
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
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.renew();
  }

  @Test(expected = ResetException.class)
  public void txResetDeniedForAlreadyResetTransaction() {
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.reset();
    txn.renew();
    txn.reset();
    txn.reset();
  }

  @Test(expected = ReadOnlyRequiredException.class)
  public void txResetDeniedForReadWriteTransaction() {
    final Txn<ByteBuffer> txn = env.txnWrite();
    txn.reset();
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
