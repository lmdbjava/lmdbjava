package org.lmdbjava;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class RefCounterTest {
  private static final long GOLDEN_RATIO = 0x9e3779b9L;

  private final int iterations = 1_000_000;
  private final int threadCount = Runtime.getRuntime().availableProcessors();
  private volatile Object env = new Object();

  @Test
  public void perfTest() {
    // Do multiple rounds to let it warm up
    for (int i = 1; i <= 3; i++) {
      System.out.println("Multi-threaded tests ---------------------------------");

      System.out.println("Round: " + i + " " + StripedRefCounter.class.getSimpleName());
      IntStream.of(1, 2, 4, 8, 16, 32, 64, 128)
          .forEach(stripes -> runTest(stripes, new StripedRefCounter(stripes)));

      System.out.println("Round: " + i + " " + SimpleRefCounter.class.getSimpleName());
      runTest(0, new SimpleRefCounter());

      System.out.println("Round: " + i + " " + NoOpRefCounter.class.getSimpleName());
      runTest(0, new NoOpRefCounter());

      System.out.println("Single-threaded tests ---------------------------------");

      System.out.println("Round: " + i + " " + StripedRefCounter.class.getSimpleName());
      runTest(1, 1, new StripedRefCounter(1));

      System.out.println("Round: " + i + " " + SimpleRefCounter.class.getSimpleName());
      runTest(0, 1, new SimpleRefCounter());

      System.out.println("Round: " + i + " " + NoOpRefCounter.class.getSimpleName());
      runTest(0, 1, new NoOpRefCounter());

      System.out.println("Round: " + i + " " + SingleThreadedRefCounter.class.getSimpleName());
      runTest(0, 1, new SingleThreadedRefCounter());
    }
  }

  @Test
  public void noOpRefCounter() {
    // Do multiple rounds to let it warm up
    for (int i = 0; i < 20; i++) {
      doNoOpRefCounter();
    }
  }

  private int goldenRatioStripeIdx(final int stripes) {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    final long idx = (Thread.currentThread().getId() * GOLDEN_RATIO) % stripes;
    return (int) idx;
  }

  private int threadLocalRandom(final int stripes) {
    // TODO In >= Java19, getId() is deprecated, so change to .threadId()
    if (stripes <= 0) {
      return -1;
    } else {
      return ThreadLocalRandom.current().nextInt(stripes);
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

  private void runTest(int stripes, final RefCounter refCounter) {
    runTest(stripes, threadCount, refCounter);
  }

  private void runTest(int stripes, final int threadCount, final RefCounter refCounter) {
//    System.out.println("Running test for " + stripes + " stripes");

    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
//    final RefCounter refCounter = new StripedRefCounterImpl(stripes, this::onClose);
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      futures[i] = CompletableFuture.runAsync(() -> {
//        if (refCounter instanceof StampedLockRefCounterImpl) {
//          final int stripeIdx = ((StampedLockRefCounterImpl) refCounter).getStripeIdx();
//          System.out.printf("stripes: %s, threadId: %s, stripeIdx: %s, goldenRatioStripe: %s, threadLocalRandom: %s%n",
//              stripes,
//              Thread.currentThread().getId(),
//              stripeIdx,
//              goldenRatioStripeIdx(stripes),
//              threadLocalRandom(stripeIdx));
//        }

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

  @Test
  void testBehaviour() throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
//    final int threadCount = 2;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final int rounds = 50;
    final int iterations = 10_000_000;

    for (int k = 0; k < rounds; k++) {
      final int round = k;
      System.out.printf("Round %s ----------------------------------------%n", round);

      // Reset the env
      env = new Object();
//      final StripedRefCounterImpl refCounter = new StripedRefCounterImpl(12, this::onClose);
      final RefCounter refCounter = new StripedRefCounter();
      final CountDownLatch startLatch = new CountDownLatch(threadCount);
      final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
      final AtomicLong[] counts = new AtomicLong[threadCount];
      for (int i = 0; i < threadCount; i++) {
        counts[i] = new AtomicLong();
      }

      final AtomicBoolean abortThreads = new AtomicBoolean(false);

      for (int i = 0; i < threadCount; i++) {
        final int threadIdx = i;
        futures[threadIdx] = CompletableFuture.runAsync(() -> {
          // Wait for all threads to be ready
          countDownThenAwait(startLatch);
          System.out.println(Thread.currentThread() + " - Starting");

          for (int j = 0; j < iterations; j++) {
            if (j % 100000 == 0) {
              System.out.println(Thread.currentThread() + ", j: " + j);
            }
            if (abortThreads.get()) {
              break;
            }

            final RefCounter.RefCounterReleaser releaser;
            try {
              releaser = refCounter.acquire();
              counts[threadIdx].incrementAndGet();
            } catch (Env.AlreadyClosedException e) {
              System.out.println(Thread.currentThread() + ", round: " + round + ", Env closed, aborting");
              break;
            }
            try {
              // Make the work between acquire and release take some time
//              Thread.sleep(random.nextInt(10));
              // env is null after closure
              Objects.requireNonNull(env, "Attempt to use a null env");
//            } catch (InterruptedException e) {
//              throw new RuntimeException(e);
            } finally {
              releaser.release();
            }
          }
        System.out.println(Thread.currentThread() + " - Done");
        }, executorService);
      }

      // Wait for all threads to start using the ref counter
      startLatch.await();

      // Give the other threads a chance to get underway
      Thread.sleep(1000 + random.nextInt(200));
      final AtomicBoolean didClose = new AtomicBoolean(false);
      final AtomicInteger closeCallCount = new AtomicInteger();
      while (!didClose.get()) {
        try {
          System.out.println("close called");
          refCounter.close(() -> {
            System.out.println("onClose called");
            env = null;
            didClose.set(true);
            closeCallCount.incrementAndGet();
          });
          if (didClose.get()) {
            // We closed, so env should be null
            assertThat(env)
                .isNull();
          }
        } catch (Env.EnvInUseException e) {
          // Failed to close so env still alive
          assertThat(env)
              .isNotNull();
          abortThreads.set(true);
          Thread.sleep(1000);
        }
      }

      // Wait for all workers to finish
      CompletableFuture.allOf(futures).join();

      System.out.println("Acquire call count: " + Arrays.stream(counts)
          .mapToLong(AtomicLong::get)
          .sum());

      assertThat(env)
          .isNull();
      assertThat(refCounter.isClosed())
          .isEqualTo(true);
      assertThatThrownBy(refCounter::getCount)
          .isInstanceOf(Env.AlreadyClosedException.class);
      assertThat(closeCallCount)
          .hasValue(1);
    }
  }

  @Test
  void testGetCount() throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final int rounds = 5;
    final int iterations = 1_000_000;

    for (int k = 0; k < rounds; k++) {
//      final int round = k;
      System.out.printf("Round %s ----------------------------------------%n", k);

      // Reset the env
      env = new Object();
//      final StripedRefCounterImpl refCounter = new StripedRefCounterImpl(12, this::onClose);
      final RefCounter refCounter = new StripedRefCounter();
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

      for (int i = 0; i < 10; i++) {
        try {
//          refCounter.close();
          System.out.println("count: " + refCounter.getCount());
        } catch (Env.EnvInUseException e) {
          Thread.sleep(100 + random.nextInt(1000));
        }
      }

      // Wait for all workers to finish
      CompletableFuture.allOf(futures).join();

      System.out.println("Acquire call count: " + Arrays.stream(counts).sum());

      if (refCounter.getCount() != 0) {
        throw new IllegalStateException("Ref count is " + refCounter.getCount());
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
