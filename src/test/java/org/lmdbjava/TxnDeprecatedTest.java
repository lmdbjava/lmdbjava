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
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TxnFlags.MDB_RDONLY_TXN;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Env.AlreadyClosedException;
import org.lmdbjava.Txn.IncompatibleParent;

/**
 * Tests all the deprecated txn related methods in {@link Env}. Essentially a duplicate of {@link
 * TxnTest}. When all the deprecated methods are deleted we can delete this test class.
 *
 * @deprecated Tests all the deprecated txn related methods in {@link Env}.
 */
@Deprecated
public final class TxnDeprecatedTest {

  private Path file;
  private Env<ByteBuffer> env;

  private TempDir tempDir;

  @BeforeEach
  void beforeEach() {
    tempDir = new TempDir();
    file = tempDir.createTempFile();
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
    tempDir.cleanup();
  }

  @Test
  public void txParent() {
    try (Txn<ByteBuffer> txRoot = env.txnWrite();
        Txn<ByteBuffer> txChild = env.txn(txRoot, new TxnFlags[0])) {
      assertThat(txRoot.getParent()).isNull();
      assertThat(txChild.getParent()).isEqualTo(txRoot);
    }
  }

  @Test
  public void txParent2() {
    try (Txn<ByteBuffer> txRoot = env.txnWrite();
        Txn<ByteBuffer> txChild = env.txn(txRoot, (TxnFlags[]) null)) {
      assertThat(txRoot.getParent()).isNull();
      assertThat(txChild.getParent()).isEqualTo(txRoot);
    }
  }

  @Test
  void txParentDeniedIfEnvClosed() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txRoot = env.txnWrite();
                  Txn<ByteBuffer> txChild = env.txn(txRoot, new TxnFlags[0])) {
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
                env.txn(txRoot, new TxnFlags[0]); // error
              }
            })
        .isInstanceOf(IncompatibleParent.class);
  }

  @Test
  void txParentRWChildROIncompatible() {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txRoot = env.txnWrite()) {
                TxnFlags[] flags = new TxnFlags[] {MDB_RDONLY_TXN};
                env.txn(txRoot, flags); // error
              }
            })
        .isInstanceOf(IncompatibleParent.class);
  }
}
