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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env.NotOpenException;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;
import org.lmdbjava.Txn.CommittedException;
import org.lmdbjava.Txn.IncompatibleParent;
import org.lmdbjava.Txn.NotResetException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import org.lmdbjava.Txn.ReadWriteRequiredException;
import org.lmdbjava.Txn.ResetException;
import static org.lmdbjava.TxnFlags.MDB_RDONLY;

public class TxnTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<ByteBuffer> env;

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = create()
      .setMapSize(10, ByteUnit.KIBIBYTES)
      .setMaxReaders(1)
      .setMaxDbs(2)
      .open(path, POSIX_MODE, MDB_NOSUBDIR);
  }

  @Test(expected = CommittedException.class)
  public void testCheckNotCommitted() {
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.commit();
    txn.checkNotCommitted();
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
  @Ignore("Travis CI failure; suspect older liblmdb version")
  public void testGetId() {
    Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);

    final AtomicLong txId1 = new AtomicLong();
    final AtomicLong txId2 = new AtomicLong();

    try (final Txn<ByteBuffer> tx1 = env.txnRead()) {
      txId1.set(tx1.getId());
    }

    db.put(bb(1), bb(2));

    try (final Txn<ByteBuffer> tx2 = env.txnRead()) {
      txId2.set(tx2.getId());
    }
    // should not see the same snapshot
    assertThat(txId1.get(), is(not(txId2.get())));
  }

  @Test
  public void txCanCommitThenCloseWithoutError() {
    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.isCommitted(), is(false));
      txn.commit();
      assertThat(txn.isCommitted(), is(true));
    }
  }

  @Test(expected = CommittedException.class)
  public void txCannotAbortIfAlreadyCommitted() {
    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.isCommitted(), is(false));
      txn.commit();
      assertThat(txn.isCommitted(), is(true));
      txn.abort();
    }
  }

  @Test(expected = CommittedException.class)
  public void txCannotCommitTwice() {
    final Txn<ByteBuffer> txn = env.txnRead();
    txn.commit();
    txn.commit(); // error
  }

  @Test(expected = NotOpenException.class)
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
    env.txn(txRoot, MDB_RDONLY); // error
  }

  @Test
  public void txReadOnly() {
    final Txn<ByteBuffer> txn = env.txnRead();
    assertThat(txn.getParent(), is(nullValue()));
    assertThat(txn.isCommitted(), is(false));
    assertThat(txn.isReadOnly(), is(true));
    assertThat(txn.isReset(), is(false));
    txn.checkNotCommitted();
    txn.checkReadOnly();
    txn.reset();
    assertThat(txn.isReset(), is(true));
    txn.renew();
    assertThat(txn.isReset(), is(false));
    txn.commit();
    assertThat(txn.isCommitted(), is(true));
  }

  @Test
  public void txReadWrite() {
    final Txn<ByteBuffer> txn = env.txnWrite();
    assertThat(txn.getParent(), is(nullValue()));
    assertThat(txn.isCommitted(), is(false));
    assertThat(txn.isReadOnly(), is(false));
    txn.checkNotCommitted();
    txn.checkWritesAllowed();
    txn.commit();
    assertThat(txn.isCommitted(), is(true));
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
}
