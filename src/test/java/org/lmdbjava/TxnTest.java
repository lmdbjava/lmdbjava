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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_RDONLY_ENV;
import static org.lmdbjava.KeyRange.closed;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.Txn.State.DONE;
import static org.lmdbjava.Txn.State.READY;
import static org.lmdbjava.Txn.State.RELEASED;
import static org.lmdbjava.Txn.State.RESET;
import static org.lmdbjava.TxnFlags.MDB_RDONLY_TXN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Dbi.BadValueSizeException;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Txn.EnvIsReadOnly;
import org.lmdbjava.Txn.IncompatibleParent;
import org.lmdbjava.Txn.NotReadyException;
import org.lmdbjava.Txn.NotResetException;
import org.lmdbjava.Txn.ReadOnlyRequiredException;
import org.lmdbjava.Txn.ReadWriteRequiredException;
import org.lmdbjava.Txn.ResetException;

/** Test {@link Txn}. */
public final class TxnTest {

  private Path file;
  private Env<ByteBuffer> env;

  @BeforeEach
  void beforeEach() {
    file = FileUtil.createTempFile();
    env =
        create()
            .setMapSize(256, ByteUnit.KIBIBYTES)
            .setMaxReaders(1)
            .setMaxDbs(2)
            .setEnvFlags(MDB_NOSUBDIR)
            .open(file);
  }

  @AfterEach
  void afterEach() {
    env.close();
    FileUtil.delete(file);
  }

  @Test
  void largeKeysRejected() throws IOException {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE);
              final ByteBuffer key = allocateDirect(env.getMaxKeySize() + 1);
              key.limit(key.capacity());
              dbi.put(key, bb(2));
            })
        .isInstanceOf(BadValueSizeException.class);
  }

  @Test
  void rangeSearch() {
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

      assertThat(keysFound.size()).isEqualTo(3);
    }
  }

  @Test
  void readOnlyTxnAllowedInReadOnlyEnv() {
    env.openDbi(DB_1, MDB_CREATE);
    try (Env<ByteBuffer> roEnv =
        create().setMaxReaders(1).setEnvFlags(MDB_NOSUBDIR, MDB_RDONLY_ENV).open(file)) {
      assertThat(roEnv.txnRead()).isNotNull();
    }
  }

  @Test
  void readWriteTxnDeniedInReadOnlyEnv() {
    assertThatThrownBy(
            () -> {
              env.openDbi(DB_1, MDB_CREATE);
              env.close();
              try (Env<ByteBuffer> roEnv =
                  create().setMaxReaders(1).open(file.toFile(), MDB_NOSUBDIR, MDB_RDONLY_ENV)) {
                roEnv.txnWrite(); // error
              }
            })
        .isInstanceOf(EnvIsReadOnly.class);
  }

  @Test
  void testCheckNotCommitted() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnRead()) {
                txn.commit();
                txn.checkReady();
              }
            })
        .isInstanceOf(NotReadyException.class);
  }

  @Test
  void testCheckReadOnly() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnWrite()) {
                txn.checkReadOnly();
              }
            })
        .isInstanceOf(ReadOnlyRequiredException.class);
  }

  @Test
  void testCheckWritesAllowed() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnRead()) {
                txn.checkWritesAllowed();
              }
            })
        .isInstanceOf(ReadWriteRequiredException.class);
  }

  @Test
  void testGetId() {
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
    assertThat(txId1.get()).isNotEqualTo(txId2.get());
  }

  @Test
  void txCanCommitThenCloseWithoutError() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.getState()).isEqualTo(READY);
      txn.commit();
      assertThat(txn.getState()).isEqualTo(DONE);
    }
  }

  @Test
  void txCannotAbortIfAlreadyCommitted() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnRead()) {
                assertThat(txn.getState()).isEqualTo(READY);
                txn.commit();
                assertThat(txn.getState()).isEqualTo(DONE);
                txn.abort();
              }
            })
        .isInstanceOf(NotReadyException.class);
  }

  @Test
  void txCannotCommitTwice() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnRead()) {
                txn.commit();
                txn.commit(); // error
              }
            })
        .isInstanceOf(NotReadyException.class);
  }

  @Test
  void txConstructionDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              env.close();
              env.txnRead();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void txRenewDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              final Txn<ByteBuffer> txnRead = env.txnRead();
              txnRead.close();
              env.close();
              txnRead.renew();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void txCloseDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              final Txn<ByteBuffer> txnRead = env.txnRead();
              env.close();
              txnRead.close();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void txCommitDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              final Txn<ByteBuffer> txnRead = env.txnRead();
              env.close();
              txnRead.commit();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void txAbortDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              final Txn<ByteBuffer> txnRead = env.txnRead();
              env.close();
              txnRead.abort();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void txResetDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              final Txn<ByteBuffer> txnRead = env.txnRead();
              env.close();
              txnRead.reset();
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  public void txParent() {
    try (Txn<ByteBuffer> txRoot = env.txnWrite();
        Txn<ByteBuffer> txChild = env.txn(txRoot)) {
      assertThat(txRoot.getParent()).isNull();
      assertThat(txChild.getParent()).isEqualTo(txRoot);
    }
  }

  @Test
  void txParentDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txRoot = env.txnWrite();
                  Txn<ByteBuffer> txChild = env.txn(txRoot)) {
                env.close();
                assertThat(txChild.getParent()).isEqualTo(txRoot);
              }
            })
        .isInstanceOf(AlreadyClosedException.class);
  }

  @Test
  void txParentROChildRWIncompatible() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txRoot = env.txnRead()) {
                env.txn(txRoot); // error
              }
            })
        .isInstanceOf(IncompatibleParent.class);
  }

  @Test
  void txParentRWChildROIncompatible() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txRoot = env.txnWrite()) {
                env.txn(txRoot, MDB_RDONLY_TXN); // error
              }
            })
        .isInstanceOf(IncompatibleParent.class);
  }

  @Test
  void txReadOnly() {
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertThat(txn.getParent()).isNull();
      assertThat(txn.getState()).isEqualTo(READY);
      assertThat(txn.isReadOnly()).isTrue();
      txn.checkReady();
      txn.checkReadOnly();
      txn.reset();
      assertThat(txn.getState()).isEqualTo(RESET);
      txn.renew();
      assertThat(txn.getState()).isEqualTo(READY);
      txn.commit();
      assertThat(txn.getState()).isEqualTo(DONE);
      txn.close();
      assertThat(txn.getState()).isEqualTo(RELEASED);
    }
  }

  @Test
  void txReadWrite() {
    final Txn<ByteBuffer> txn = env.txnWrite();
    assertThat(txn.getParent()).isNull();
    assertThat(txn.getState()).isEqualTo(READY);
    assertThat(txn.isReadOnly()).isFalse();
    txn.checkReady();
    txn.checkWritesAllowed();
    txn.commit();
    assertThat(txn.getState()).isEqualTo(DONE);
    txn.close();
    assertThat(txn.getState()).isEqualTo(RELEASED);
  }

  @Test
  void txRenewDeniedWithoutPriorReset() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnRead()) {
                txn.renew();
              }
            })
        .isInstanceOf(NotResetException.class);
  }

  @Test
  void txResetDeniedForAlreadyResetTransaction() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnRead()) {
                txn.reset();
                txn.renew();
                txn.reset();
                txn.reset();
              }
            })
        .isInstanceOf(ResetException.class);
  }

  @Test
  void txResetDeniedForReadWriteTransaction() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txn = env.txnWrite()) {
                txn.reset();
              }
            })
        .isInstanceOf(ReadOnlyRequiredException.class);
  }

  @Test
  void zeroByteKeysRejected() {
    assertThatThrownBy(
            () -> {
              final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, MDB_CREATE);
              final ByteBuffer key = allocateDirect(4);
              key.putInt(1);
              assertThat(key.remaining()).isEqualTo(0); // because key.flip() skipped
              dbi.put(key, bb(2));
            })
        .isInstanceOf(BadValueSizeException.class);
  }
}
