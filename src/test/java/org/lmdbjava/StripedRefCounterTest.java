package org.lmdbjava;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class StripedRefCounterTest {

  private final int threadCount = Runtime.getRuntime().availableProcessors();

  @Test
  void acquire() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    // Acquire twice
    final RefCounter.RefCounterReleaser releaser1 = stripedRefCounter.acquire();
    assertThat(stripedRefCounter.getCount())
        .isEqualTo(1);
    final RefCounter.RefCounterReleaser releaser2 = stripedRefCounter.acquire();
    assertThat(stripedRefCounter.getCount())
        .isEqualTo(2);

    final AtomicInteger onCloseCallCount = new AtomicInteger();

    // Close not called as 2 un-released
    Assertions.assertThatThrownBy(
            () -> {
              stripedRefCounter.close(onCloseCallCount::incrementAndGet);
            })
        .isInstanceOf(Env.EnvInUseException.class)
        .hasMessageContaining(" 2 ");
    assertThat(onCloseCallCount)
        .hasValue(0);

    // Release 1st releaser
    releaser1.release();
    assertThat(stripedRefCounter.getCount())
        .isEqualTo(1);

    // Close not called as 1 un-released
    Assertions.assertThatThrownBy(
            () -> {
              stripedRefCounter.close(onCloseCallCount::incrementAndGet);
            })
        .isInstanceOf(Env.EnvInUseException.class)
        .hasMessageContaining(" 1 ");
    assertThat(onCloseCallCount)
        .hasValue(0);

    // Release 2nd releaser
    releaser2.release();
    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);

    // no-op if already released
    releaser1.release();
    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);

    // no-op if already released
    releaser2.release();
    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);

    // onClose is called now
    stripedRefCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount)
        .hasValue(1);

    // no-op as onClose already called
    stripedRefCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount)
        .hasValue(1);
  }

  @Test
  void multipleThreads() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    final int iterations = 100;
    final AtomicInteger[] callCounts = new AtomicInteger[threadCount];
    for (int i = 0; i < threadCount; i++) {
      callCounts[i] = new AtomicInteger();
    }

    IntStream.range(0, threadCount)
        .boxed()
        .map(i -> CompletableFuture.runAsync(() -> {
          for (int j = 0; j < iterations; j++) {
            final RefCounter.RefCounterReleaser releaser = stripedRefCounter.acquire();
            callCounts[i].getAndIncrement();
            releaser.release();
          }
        }))
        .forEach(CompletableFuture::join);

    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);

    for (AtomicInteger callCount : callCounts) {
      assertThat(callCount)
          .hasValue(iterations);
    }
  }

  @Test
  void multipleThreads_delayedRelease() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    final int threads = Runtime.getRuntime().availableProcessors() - 2;
    final int iterations = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    final ExecutorService executor2 = Executors.newFixedThreadPool(1);
    final AtomicInteger[] callCounts = new AtomicInteger[threads];
    for (int i = 0; i < threads; i++) {
      callCounts[i] = new AtomicInteger();
    }

    final Queue<RefCounter.RefCounterReleaser> releasers = new ConcurrentLinkedQueue<>();
    final Queue<CompletableFuture<?>> futures = new ConcurrentLinkedQueue<>();

    IntStream.range(0, threads)
        .boxed()
        .map(i -> CompletableFuture.runAsync(() -> {
          for (int j = 0; j < iterations; j++) {
            final RefCounter.RefCounterReleaser releaser = stripedRefCounter.acquire();
            releasers.add(releaser);
            callCounts[i].getAndIncrement();
            futures.add(CompletableFuture.runAsync(() -> {
              final int count = stripedRefCounter.getCount();
//              System.out.println(Thread.currentThread() + " - getting count: " + count);
              assertThat(count)
                  .isNotEqualTo(0);
            }, executor2));
          }
        }, executor))
        .forEach(CompletableFuture::join);

    assertThat(stripedRefCounter.getCount())
        .isEqualTo(threads * iterations);

    for (AtomicInteger callCount : callCounts) {
      assertThat(callCount)
          .hasValue(iterations);
    }

    releasers.forEach(RefCounter.RefCounterReleaser::release);

    futures.forEach(CompletableFuture::join);

    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);
  }

  @Test
  void testImmediateClose() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    assertThat(stripedRefCounter.isClosed())
        .isEqualTo(false);
    final AtomicInteger onCloseCallCount = new AtomicInteger();

    stripedRefCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount)
        .hasValue(1);
    assertThat(stripedRefCounter.isClosed())
        .isEqualTo(true);

    assertThatThrownBy(stripedRefCounter::checkNotClosed)
        .isInstanceOf(Env.AlreadyClosedException.class);

    // Check again as idempotent
    stripedRefCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount)
        .hasValue(1);
    assertThat(stripedRefCounter.isClosed())
        .isEqualTo(true);

    assertThatThrownBy(stripedRefCounter::checkNotClosed)
        .isInstanceOf(Env.AlreadyClosedException.class);
  }

  /**
   * Lots of threads all doing acquire/release in a loop, then the main thread
   * tries to call refCounter.close(...), which will throw an
   * {@link org.lmdbjava.Env.EnvInUseException}. It then makes all worker threads
   * stop their looping and calls refCounter.close(...) again, successfully this
   * time.
   */
  @Test
  void testBehaviour() throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final int rounds = 10;
    final int iterations = 10_000_000;
    final AtomicReference<Object> mockEnv = new AtomicReference<>();

    for (int k = 0; k < rounds; k++) {
      final int round = k;
      System.out.printf("Round %s ----------------------------------------%n", round);

      // Reset the env
      mockEnv.set(new Object());
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
//          System.out.println(Thread.currentThread() + " - Starting");
          for (int j = 0; j < iterations; j++) {
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
              sleep(random.nextInt(5));
              // env is null after closure
              Objects.requireNonNull(mockEnv.get(), "Attempt to use a null env");
            } finally {
              releaser.release();
            }
          }
//          System.out.println(Thread.currentThread() + " - Done");
        }, executorService);
      }

      // Wait for all threads to start using the ref counter
      startLatch.await();

      // Give the other threads a chance to get underway
      sleep(200 + random.nextInt(200));
      final AtomicBoolean didClose = new AtomicBoolean(false);
      int closeCallCount = 0;
      final AtomicInteger onCloseCallCount = new AtomicInteger();
      while (!didClose.get()) {
        try {
          assertThat(mockEnv.get())
              .isNotNull();
          System.out.println("close called " + ++closeCallCount);
          refCounter.close(() -> {
            onCloseCallCount.incrementAndGet();
            System.out.println("onClose called " + onCloseCallCount.get());
            // Imitate closing the env
            mockEnv.set(null);
            didClose.set(true);
          });
          if (didClose.get()) {
            // We closed, so env should be null
            assertThat(mockEnv)
                .hasNullValue();
          }
        } catch (Env.EnvInUseException e) {
          // Failed to close as there are un-released items, so env still alive
          assertThat(mockEnv.get())
              .isNotNull();
          // Now poke all the treads to make them cleanly finish what they are doing so we
          // can try close() again
          abortThreads.set(true);
          sleep(500);
        }
      }

      // Wait for all workers to finish
      CompletableFuture.allOf(futures).join();

      System.out.println("Acquire call count: " + Arrays.stream(counts)
          .mapToLong(AtomicLong::get)
          .sum());

      // Make sure the mock env is all closed down
      assertThat(mockEnv)
          .hasNullValue();
      assertThat(refCounter.isClosed())
          .isEqualTo(true);
      assertThatThrownBy(refCounter::getCount)
          .isInstanceOf(Env.AlreadyClosedException.class);
      assertThatThrownBy(refCounter::acquire)
          .isInstanceOf(Env.AlreadyClosedException.class);
      assertThat(onCloseCallCount)
          .hasValue(1);
    }
  }

  /**
   * Ensure we can call getCount when multiple threads are all calling acquire/release
   * in a loop.
   */
  @Test
  void testGetCount() throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final int rounds = 5;
    final int iterations = 10_000_000;
    final AtomicReference<Object> mockEnv = new AtomicReference<>();
    final AtomicBoolean abortThreads = new AtomicBoolean(false);

    for (int k = 0; k < rounds; k++) {
//      final int round = k;
      System.out.printf("Round %s ----------------------------------------%n", k);

      // Reset the env
      mockEnv.set(new Object());
      abortThreads.set(false);
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
            if (abortThreads.get()) {
              break;
            }
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
              sleep(random.nextInt(5));
              // env is null after closure
              Objects.requireNonNull(mockEnv.get(), "Attempt to use a null env");
            } finally {
              releaser.release();
            }
            // Random sleep after releasing so there is a time when the thread
            // is not using the 'env'
            sleep(5 + random.nextInt(5));
          }
//        System.out.println(Thread.currentThread() + " - Done");
        }, executorService);
      }

      // Wait for all threads to start using the ref counter
      startLatch.await();

      // Give the other threads a chance to get underway
      sleep(100 + random.nextInt(200));

      for (int i = 0; i < 10; i++) {
        try {
          System.out.println("count: " + refCounter.getCount());
        } catch (Env.EnvInUseException e) {
          sleep(100 + random.nextInt(200));
        }
      }
      abortThreads.set(true);
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

  private static void sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
