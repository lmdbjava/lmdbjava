package org.lmdbjava;

import org.junit.jupiter.api.RepeatedTest;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.EnvFlags.MDB_NOTLS;

public class TestSegfault {
  //  @Test
  @RepeatedTest(1000)
  void testSegFault() {
    try (final ExecutorService executor1 = Executors.newSingleThreadExecutor()) {
      try (final ExecutorService executor2 = Executors.newSingleThreadExecutor()) {
        executor1.execute(makeRunnable(this::txParentDeniedIfEnvClosed));
        executor2.execute(makeRunnable(this::txParentDeniedIfEnvClosed));
        executor1.execute(makeRunnable(this::txParentDeniedIfEnvClosed));
//        executor2.execute(makeRunnable(this::txParentROChildRWIncompatible));
      }
    }
  }

  private Runnable makeRunnable(final Consumer<Env<ByteBuffer>> consumer) {
    return () -> {
      final Path file = FileUtil.createTempFile();
        try {
          try (Env<ByteBuffer> env = Env.create().setMapSize(2_085_760_999).setMaxDbs(1).open(file.toFile(), MDB_NOSUBDIR)) {
            consumer.accept(env);
          }
        } catch (final Exception e) {
          e.printStackTrace();
        } finally {
          FileUtil.delete(file);
        }
    };
  }

  private void txParentDeniedIfEnvClosed(final Env<ByteBuffer> env) {
    assertThatThrownBy(
            () -> {
              try (Txn<ByteBuffer> txRoot = env.txnWrite()) {
                try (final Txn<ByteBuffer> txChild = env.txn(txRoot)) {
                  env.close();
                  assertThat(txChild.getParent()).isEqualTo(txRoot);
                }
              }
            })
            .isInstanceOf(Env.AlreadyClosedException.class);
  }
}

