package org.lmdbjava;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class TestSegfault {

  private TempDir tempDir;

  @BeforeEach
  void setUp() {
    tempDir = new TempDir();
  }

  @AfterEach
  void tearDown() {
    tempDir.cleanup();
  }

  @Test
  void test() throws ExecutionException, InterruptedException {
    final Path file = tempDir.createTempFile();
    final Env<ByteBuffer> env =
        Env.create()
            .setMapSize(100, ByteUnit.KIBIBYTES)
            .setMaxDbs(1)
            .setEnvFlags(MDB_NOSUBDIR, EnvFlags.MDB_NOTLS)
            .open(file);

    try (final ExecutorService executor = Executors.newFixedThreadPool(11)) {

      final CountDownLatch allReadyLatch = new CountDownLatch(11);
      final List<CompletableFuture<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  final Txn<ByteBuffer> txn = env.txnRead();
                  System.out.println(Thread.currentThread().getName() + " - Opened txn");
                  allReadyLatch.countDown();
                  try {
                    allReadyLatch.await();
                    Thread.sleep(500);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  System.out.println(Thread.currentThread().getName() + " - Closing txn");
                  try {
                    txn.close();
                  } catch (Env.AlreadyClosedException e) {
                    System.out.println(
                        Thread.currentThread().getName() + " - ERROR Env already closed");
                  }
                  System.out.println(Thread.currentThread().getName() + " - Done");
                },
                executor));
      }

      futures.add(
          CompletableFuture.runAsync(
              () -> {
                allReadyLatch.countDown();
                try {
                  allReadyLatch.await();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                // Try to close before
                System.out.println(Thread.currentThread().getName() + " - Closing env");
                Assertions.assertThatThrownBy(
                        () -> {
                          env.close();
                        })
                    .isInstanceOf(Env.OpenItemsException.class);
                System.out.println(Thread.currentThread().getName() + " - Done");
              },
              executor));

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

      // Should work now
      env.close();

      System.out.println("Done");
    }
  }

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
      final Path file = tempDir.createTempFile();
      try {
        try (Env<ByteBuffer> env =
            Env.create()
                .setMapSize(2, ByteUnit.GIBIBYTES)
                .setMaxDbs(1)
                .setEnvFlags(MDB_NOSUBDIR)
                .open(file)) {
          consumer.accept(env);
        }
      } catch (final Exception e) {
        e.printStackTrace();
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
        .isInstanceOf(Env.OpenItemsException.class);
  }
}
