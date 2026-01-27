package org.lmdbjava;


import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class RefCounterTest {

  private final int iterations = 1_000_000;
  private final int threadCount = Runtime.getRuntime().availableProcessors();
  private volatile Object env = new Object();

  @Test
  public void perfTest() {
    IntStream.of(1, 2, 5, 10, 12, 14, 16, 18, 20, 22, threadCount, threadCount * 2, threadCount * 4)
        .forEach(this::runTest);
  }

  private void runTest(final int stripes) {
//    System.out.println("Running test for " + stripes + " stripes");

    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
    final StripedRefCounterImpl refCounter = new StripedRefCounterImpl(stripes, this::onClose);
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

    if (refCounter.getTotalCount() != 0) {
      throw new IllegalStateException("Ref count is " + refCounter.getTotalCount());
    }

    final Duration duration = Duration.between(startTime.get(), Instant.now());
    final double iterationsPerSec = (double) iterations / duration.toMillis() * 1000;

    System.out.println("All Finished"
        + ", stripes: " + stripes
        + ", threads: " + threadCount
        + ", duration: " + duration
        + ", iterationsPerSec: " + iterationsPerSec);
  }

  @Test
  void testBehaviour() throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final int rounds = 50;
    final int iterations = 100_000_000;

    for (int k = 0; k < rounds; k++) {
      final int round = k;
      System.out.printf("Round %s ----------------------------------------%n", round);

      // Reset the env
      env = new Object();
      final StripedRefCounterImpl refCounter = new StripedRefCounterImpl(12, this::onClose);
      final CountDownLatch startLatch = new CountDownLatch(threadCount);
      final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
      final long[] counts = new long[threadCount];

      for (int i = 0; i < threadCount; i++) {
        final int threadIdx = i;
        futures[threadIdx] = CompletableFuture.runAsync(() -> {
          // Wait for all threads to be ready
          countDownThenAwait(startLatch);
//        System.out.println(Thread.currentThread() + " - Starting");

          for (int j = 0; j < iterations; j++) {
            final RefCounter.RefCounterReleaser releaser;
            try {
              releaser = refCounter.acquire();
              counts[threadIdx]++;
            } catch (Env.AlreadyClosedException e) {
//              System.out.println(Thread.currentThread() + ", round: " + round + ", Env closed, aborting");
              break;
            }
            try {
              // Make the work between acquire and release take some time
              Thread.sleep(random.nextInt(1));
              // env is null after closure
              Objects.requireNonNull(env, "Attempt to use a null env");
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } finally {
              releaser.release();
            }
          }
//        System.out.println(Thread.currentThread() + " - Done");
        }, executorService);
      }

      // Wait for all threads to start using the ref counter
      startLatch.await();

      // Give the other threads a chance to get underway
      Thread.sleep(100 + random.nextInt(200));

      while (true) {
        try {
          refCounter.close();
          break;
        } catch (Env.EnvInUseException e) {
          Thread.sleep(100);
        }
      }

      // Wait for all workers to finish
      CompletableFuture.allOf(futures).join();

      System.out.println("Acquire call count: " + Arrays.stream(counts).sum());

      if (refCounter.getTotalCount() != 0) {
        throw new IllegalStateException("Ref count is " + refCounter.getTotalCount());
      }

      if (!refCounter.isClosed()) {
        throw new IllegalStateException("Env not closed");
      }
    }
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

  private void onClose() {
    System.out.println(Thread.currentThread() + " - Starting onClose runnable");
    env = null;
    System.out.println(Thread.currentThread() + " - Finishing onClose runnable");
  }

}
