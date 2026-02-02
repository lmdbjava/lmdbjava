package org.lmdbjava;


import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class RefCounterTest {
  private static final long GOLDEN_RATIO = 0x9e3779b9L;

  private final int iterations = 1_000_000;
  private final int threadCount = Runtime.getRuntime().availableProcessors();
  private volatile Object env = new Object();

  @Disabled // Manual performance test
  @Test
  public void perfTest() {
    // Do multiple rounds to let it warm up
    for (int i = 1; i <= 3; i++) {
      final int round = i;
      System.out.println("Multi-threaded (all cores) tests ---------------------------------");

      System.out.println("Round: " + round + " " + StripedRefCounter.class.getSimpleName());
      IntStream.of(1, 2, 4, 8, 16, 32, 64, 128)
          .forEach(stripes -> runPerfTest(stripes, new StripedRefCounter(stripes)));

      System.out.println("Round: " + round + " " + SimpleRefCounter.class.getSimpleName());
      runPerfTest(0, new SimpleRefCounter());

      System.out.println("Round: " + round + " " + NoOpRefCounter.class.getSimpleName());
      runPerfTest(0, new NoOpRefCounter());

      IntStream.of(2, 4, 8)
          .forEach(threads -> {
            System.out.println("Multi-threaded (" + threads + " threads) tests ---------------------------------");

            System.out.println("Round: " + round + " " + StripedRefCounter.class.getSimpleName());
            IntStream.of(1, 2, 4, 8, 16, 32, 64, 128)
                .forEach(stripes -> runPerfTest(stripes, new StripedRefCounter(stripes)));

            System.out.println("Round: " + round + " " + SimpleRefCounter.class.getSimpleName());
            runPerfTest(0, new SimpleRefCounter());

            System.out.println("Round: " + round + " " + NoOpRefCounter.class.getSimpleName());
            runPerfTest(0, new NoOpRefCounter());
              });

      System.out.println("Single-threaded tests ---------------------------------");

      System.out.println("Round: " + round + " " + StripedRefCounter.class.getSimpleName());
      IntStream.of(1, 2, 4, 8, 16, 32, 64, 128)
          .forEach(stripes -> runPerfTest(stripes, 1, new StripedRefCounter(stripes)));

      System.out.println("Round: " + round + " " + SimpleRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new SimpleRefCounter());

      System.out.println("Round: " + round + " " + NoOpRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new NoOpRefCounter());

      System.out.println("Round: " + round + " " + SingleThreadedRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new SingleThreadedRefCounter());


      System.out.println("--------------------------------------------------------------------------------");
      System.out.println();
    }
  }

  @Test
  public void noOpRefCounter() {
    // Do multiple rounds to let it warm up
    for (int i = 0; i < 20; i++) {
      doNoOpRefCounter();
    }
  }

  private void doNoOpRefCounter() {
//    System.out.println("Running test for " + stripes + " stripes");

    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
    final NoOpRefCounter refCounter = new NoOpRefCounter();
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      futures[i] = CompletableFuture.runAsync(() -> {
        // Wait for all threads to be ready
        countDownThenAwait(startLatch);

        // Capture the start time
        startTime.updateAndGet(currVal -> {
          if (currVal == null) {
            return Instant.now();
          } else {
            return currVal;
          }
        });

        for (int j = 0; j < iterations; j++) {
          final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
          try {
            // Make sure we have an env that is not 'closed'
            Objects.requireNonNull(env);
          } finally {
            releaser.release();
          }
        }
//        System.out.println(Thread.currentThread() + " - Done");
      }, executorService);
    }
    CompletableFuture.allOf(futures).join();

    final Duration duration = Duration.between(startTime.get(), Instant.now());
    final double iterationsPerSec = (double) iterations / duration.toMillis() * 1000;

    System.out.println("All Finished"
        + ", threads: " + threadCount
        + ", duration: " + duration
        + ", iterationsPerSec: " + iterationsPerSec);
  }

  private void runPerfTest(int stripes, final RefCounter refCounter) {
    runPerfTest(stripes, threadCount, refCounter);
  }

  private void runPerfTest(int stripes, final int threadCount, final RefCounter refCounter) {
    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      futures[i] = CompletableFuture.runAsync(() -> {
        // Wait for all threads to be ready
        countDownThenAwait(startLatch);
        // Capture the start time
        startTime.updateAndGet(currVal -> {
          if (currVal == null) {
            return Instant.now();
          } else {
            return currVal;
          }
        });

        for (int j = 0; j < iterations; j++) {
          final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
          releaser.release();
        }
      }, executorService);
    }
    CompletableFuture.allOf(futures).join();

    if (refCounter.getCount() != 0) {
      throw new IllegalStateException("Ref count is " + refCounter.getCount());
    }

    final Duration duration = Duration.between(startTime.get(), Instant.now());
    final double iterationsPerSec = (double) iterations / duration.toMillis() * 1000;

    System.out.println("All Finished"
        + ", stripes: " + stripes
        + ", threads: " + threadCount
        + ", duration: " + duration
        + ", iterationsPerSec: " + iterationsPerSec);
  }

  private void countDownThenAwait(final CountDownLatch latch) {
    latch.countDown();
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
