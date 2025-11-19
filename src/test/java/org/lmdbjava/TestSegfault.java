package org.lmdbjava;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.junit.jupiter.api.RepeatedTest;

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
      try (final TempDir tempDir = new TempDir()) {
        try {
          try (Env<ByteBuffer> env =
              Env.create()
                  .setMapSize(2_085_760_999)
                  .addEnvFlag(MDB_NOSUBDIR)
                  .setMaxDbs(1)
                  .open(tempDir.createTempFile())) {
            consumer.accept(env);
          }
        } catch (final Exception e) {
          e.printStackTrace();
        }
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
