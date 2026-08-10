/*
 * Copyright © 2016-2026 The LmdbJava Open Source Project
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

import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
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
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RefCounterTest {
  private static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();
  private final int iterations = 20_000_000;
  private final int threadCount = PROCESSOR_COUNT;
  private volatile Object env = new Object();

  @Disabled // Manual performance test
  @Test
  public void perfTest() {
    // Do multiple rounds to let it warm up
    for (int i = 1; i <= 3; i++) {
      final int round = i;
      // Run tests with all available processors
      System.out.println(
          "Multi-threaded (" + threadCount + " threads) tests ---------------------------------");

      System.out.println("Round: " + round + " " + StripedRefCounter.class.getSimpleName());
      IntStream.of(1, 16, 32, 64, 128, 256)
          .forEach(stripes -> runPerfTest(stripes, new StripedRefCounter(stripes)));

      final StripedRefCounter defaultStripedRefCounter = new StripedRefCounter();
      runPerfTest(defaultStripedRefCounter.getStripeCount(), defaultStripedRefCounter);

      System.out.println("Round: " + round + " " + SimpleRefCounter.class.getSimpleName());
      runPerfTest(0, new SimpleRefCounter());

      System.out.println("Round: " + round + " " + SynchronisedRefCounter.class.getSimpleName());
      runPerfTest(0, new SynchronisedRefCounter());

      System.out.println("Round: " + round + " " + NoOpRefCounter.class.getSimpleName());
      runPerfTest(0, new NoOpRefCounter());

      // Run tests with set numbers of worker threads
      IntStream.of(32, 16, 8, 4, 2)
          .forEach(
              threads -> {
                System.out.println(
                    "Multi-threaded ("
                        + threads
                        + " threads) tests ---------------------------------");

                System.out.println(
                    "Round: " + round + " " + StripedRefCounter.class.getSimpleName());
                IntStream.of(1, 16, 32, 64, 128, 256)
                    .forEach(
                        stripes -> runPerfTest(stripes, threads, new StripedRefCounter(stripes)));

                System.out.println(
                    "Round: " + round + " " + SimpleRefCounter.class.getSimpleName());
                runPerfTest(0, threads, new SimpleRefCounter());

                System.out.println(
                    "Round: " + round + " " + SynchronisedRefCounter.class.getSimpleName());
                runPerfTest(0, threads, new SynchronisedRefCounter());

                System.out.println("Round: " + round + " " + NoOpRefCounter.class.getSimpleName());
                runPerfTest(0, threads, new NoOpRefCounter());
              });

      System.out.println("Single-threaded tests ---------------------------------");

      System.out.println("Round: " + round + " " + StripedRefCounter.class.getSimpleName());
      runPerfTest(1, 1, new StripedRefCounter());

      System.out.println("Round: " + round + " " + SimpleRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new SimpleRefCounter());

      System.out.println("Round: " + round + " " + SynchronisedRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new SynchronisedRefCounter());

      System.out.println("Round: " + round + " " + NoOpRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new NoOpRefCounter());

      System.out.println("Round: " + round + " " + SingleThreadedRefCounter.class.getSimpleName());
      runPerfTest(0, 1, new SingleThreadedRefCounter());

      System.out.println(
          "--------------------------------------------------------------------------------");
      System.out.println();
    }
  }

  /**
   * @return A {@link Stream} of all {@link RefCounter}s
   */
  static Stream<Arguments> allRefCounterProvider() {
    return Stream.of(
            new StripedRefCounter(),
            new SingleThreadedRefCounter(),
            new SimpleRefCounter(),
            new SynchronisedRefCounter(),
            new NoOpRefCounter())
        .map(refCounter -> Arguments.argumentSet(
            refCounter.getClass().getSimpleName(),
            refCounter));
  }

  /**
   * @return A {@link Stream} of {@link RefCounter}s that support multi-threaded use
   */
  static Stream<Arguments> multiThreadedRefCounterProvider() {
    return Stream.of(
            new StripedRefCounter(),
            new SimpleRefCounter(),
            new SynchronisedRefCounter())
        .map(refCounter -> Arguments.argumentSet(
            refCounter.getClass().getSimpleName(),
            refCounter));
  }

  private void assertRefCount(final RefCounter refCounter, final int expectedCount) {
    // NoOpRefCounter does no reference counting, so we can't assert the count
    if (!(refCounter instanceof NoOpRefCounter)) {
      assertThat(refCounter.getCount()).isEqualTo(expectedCount);
    }
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void testRefCounters(final RefCounter refCounter) {
    // Acquire twice
    final RefCounter.RefCounterReleaser releaser1 = refCounter.acquire();
    assertRefCount(refCounter, 1);
    final RefCounter.RefCounterReleaser releaser2 = refCounter.acquire();
    assertRefCount(refCounter, 2);

    final AtomicInteger onCloseCallCount = new AtomicInteger();

    if (!(refCounter instanceof NoOpRefCounter)) {
      // Close not called as 2 are un-released
      Assertions.assertThatThrownBy(
              () -> {
                refCounter.close(onCloseCallCount::incrementAndGet);
              })
          .isInstanceOf(Env.EnvInUseException.class)
          .hasMessageContaining(" 2 ");
    }
    assertThat(onCloseCallCount).hasValue(0);

    // Release 1st releaser
    releaser1.release();
    assertRefCount(refCounter, 1);

    if (!(refCounter instanceof NoOpRefCounter)) {
      // Close not called as 1 un-released
      Assertions.assertThatThrownBy(
              () -> {
                refCounter.close(onCloseCallCount::incrementAndGet);
              })
          .isInstanceOf(Env.EnvInUseException.class)
          .hasMessageContaining(" 1 ");
    }
    assertThat(onCloseCallCount).hasValue(0);

    // Release 2nd releaser
    releaser2.release();
    assertThat(refCounter.getCount()).isEqualTo(0);

    // no-op if already released
    releaser1.release();
    assertThat(refCounter.getCount()).isEqualTo(0);

    // no-op if already released
    releaser2.release();
    assertThat(refCounter.getCount()).isEqualTo(0);

    // onClose is called now
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount).hasValue(1);

    // no-op as onClose already called
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount).hasValue(1);
  }

  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void multipleThreads(final RefCounter refCounter) {
    final int iterations = 1000;
    final AtomicInteger[] callCounts = new AtomicInteger[threadCount];
    for (int i = 0; i < threadCount; i++) {
      callCounts[i] = new AtomicInteger();
    }
    final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
    try (ExecutorService executorService = Executors.newFixedThreadPool(threadCount)) {

      final CompletableFuture<?>[] futures = IntStream.range(0, threadCount)
          .boxed()
          .map(
              i ->
                  CompletableFuture.runAsync(
                      () -> {
                        TestUtils.countDownThenAwait(countDownLatch);
                        for (int j = 0; j < iterations; j++) {
                          final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
                          callCounts[i].getAndIncrement();
                          releaser.release();
                        }
                      },
                      executorService))
          .toArray(CompletableFuture[]::new);

      CompletableFuture.allOf(futures).join();

      assertThat(refCounter.getCount()).isEqualTo(0);

      for (AtomicInteger callCount : callCounts) {
        assertThat(callCount).hasValue(iterations);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void multipleThreads_delayedRelease(final RefCounter refCounter) {
    final int iterations = 1000;
    final AtomicInteger[] callCounts;
    final Queue<RefCounter.RefCounterReleaser> releasers;

    try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
      try (ExecutorService executor2 = Executors.newFixedThreadPool(threadCount)) {
        callCounts = new AtomicInteger[threadCount];
        for (int i = 0; i < threadCount; i++) {
          callCounts[i] = new AtomicInteger();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);

        releasers = new ConcurrentLinkedQueue<>();
        final Queue<CompletableFuture<?>> futures = new ConcurrentLinkedQueue<>();

        IntStream.range(0, threadCount)
            .boxed()
            .map(
                i ->
                    CompletableFuture.runAsync(
                        () -> {
                          TestUtils.countDownThenAwait(countDownLatch);
                          for (int j = 0; j < iterations; j++) {
                            final RefCounter.RefCounterReleaser releaser =
                                refCounter.acquire();
                            releasers.add(releaser);
                            callCounts[i].getAndIncrement();
                            futures.add(
                                CompletableFuture.runAsync(
                                    () -> {
                                      final long count = refCounter.getCount();
                                      assertThat(count).isNotEqualTo(0);
                                    },
                                    executor2));
                          }
                        },
                        executor))
            .forEach(futures::add);

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      }
    }

    assertRefCount(refCounter, threadCount * iterations);

    for (AtomicInteger callCount : callCounts) {
      assertThat(callCount).hasValue(iterations);
    }

    releasers.forEach(RefCounter.RefCounterReleaser::release);

    assertThat(refCounter.getCount()).isEqualTo(0);
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void testImmediateClose(final RefCounter refCounter) {
    assertThat(refCounter.isClosed()).isEqualTo(false);
    final AtomicInteger onCloseCallCount = new AtomicInteger();

    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount).hasValue(1);
    assertThat(refCounter.isClosed()).isEqualTo(true);

    assertThatThrownBy(refCounter::checkNotClosed)
        .isInstanceOf(Env.AlreadyClosedException.class);

    // Check again as idempotent
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount).hasValue(1);
    assertThat(refCounter.isClosed()).isEqualTo(true);

    assertThatThrownBy(refCounter::checkNotClosed)
        .isInstanceOf(Env.AlreadyClosedException.class);
  }


  /**
   * Lots of threads all doing acquire/release in a loop, then the main thread tries to call
   * refCounter.close(...), which will throw an {@link org.lmdbjava.Env.EnvInUseException}. Main thread then
   * makes all worker threads stop their looping and calls refCounter.close(...) again, successfully
   * this time.
   */
  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void testBehaviour(final RefCounter refCounter) throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
    try (ExecutorService executorService = Executors.newFixedThreadPool(threadCount)) {
      final int rounds = 5;
      final int iterations = 10_000_000;
      final AtomicReference<Object> mockEnv = new AtomicReference<>();

      for (int k = 0; k < rounds; k++) {
        final int round = k;
        System.out.printf("Round %s ----------------------------------------%n", round);

        // Reset the env
        mockEnv.set(new Object());
        final RefCounter roundRefCounter = createNewRefCounter(refCounter);
        final CountDownLatch startLatch = new CountDownLatch(threadCount);
        final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
        final AtomicLong[] counts = new AtomicLong[threadCount];
        for (int i = 0; i < threadCount; i++) {
          counts[i] = new AtomicLong();
        }

        final AtomicBoolean abortThreads = new AtomicBoolean(false);

        for (int i = 0; i < threadCount; i++) {
          final int threadIdx = i;
          futures[threadIdx] =
              CompletableFuture.runAsync(
                  () -> {
                    // Wait for all threads to be ready
                    TestUtils.countDownThenAwait(startLatch);
                    //          System.out.println(Thread.currentThread() + " - Starting");
                    for (int j = 0; j < iterations; j++) {
                      if (abortThreads.get()) {
                        System.out.println(
                            Thread.currentThread() + ", round: " + round + ", j: " + j + ", abortThreads is true");
                        break;
                      }

                      final RefCounter.RefCounterReleaser releaser;
                      try {
                        releaser = roundRefCounter.acquire();
                        counts[threadIdx].incrementAndGet();
                      } catch (Env.AlreadyClosedException e) {
                        System.out.println(
                            Thread.currentThread() + ", round: " + round + ", j: " + j + ", Env closed, aborting");
                        break;
                      }
                      try {
                        // Make the work between acquire and release take some time
                        TestUtils.sleep(random.nextInt(5));
                        // env is null after closure
                        assertThat(mockEnv.get()).isNotNull();
  //                      Objects.requireNonNull(mockEnv.get(), "Attempt to use a null env");
                      } finally {
                        releaser.release();
                      }
                    }
                    //          System.out.println(Thread.currentThread() + " - Done");
                  },
                  executorService);
        }

        // Wait for all threads to start using the ref counter
        startLatch.await();

        // Give the other threads a chance to get underway
        TestUtils.sleep(200 + random.nextInt(200));
        final AtomicBoolean didClose = new AtomicBoolean(false);
        int closeCallCount = 0;
        final AtomicInteger onCloseCallCount = new AtomicInteger();
        while (!didClose.get()) {
          try {
            assertThat(mockEnv.get()).isNotNull();
            System.out.println("close called " + ++closeCallCount);
            roundRefCounter.close(
                () -> {
                  onCloseCallCount.incrementAndGet();
                  System.out.println("onClose called " + onCloseCallCount.get());
                  // Imitate closing the env
                  mockEnv.set(null);
                  didClose.set(true);
                });
            if (didClose.get()) {
              // We closed, so env should be null
              assertThat(mockEnv).hasNullValue();
            }
          } catch (Env.EnvInUseException e) {
            // Failed to close as there are un-released items, so env still alive
            assertThat(mockEnv.get()).isNotNull();
            // Now poke all the treads to make them cleanly finish what they are doing so we
            // can try close() again
            abortThreads.set(true);
            TestUtils.sleep(500);
          }
        }

        // Wait for all workers to finish
        CompletableFuture.allOf(futures).join();

        System.out.println(
            "Acquire call count: " + Arrays.stream(counts).mapToLong(AtomicLong::get).sum());

        // Make sure the mock env is all closed down
        assertThat(mockEnv).hasNullValue();
        assertThat(roundRefCounter.isClosed()).isEqualTo(true);
        assertThat(roundRefCounter.getCount()).isZero();
        assertThatThrownBy(roundRefCounter::acquire).isInstanceOf(Env.AlreadyClosedException.class);
        assertThat(onCloseCallCount).hasValue(1);
      }
    }
  }

  /**
   * Ensure we can call getCount when multiple threads are all calling acquire/release in a loop.
   */
  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void testGetCount(final RefCounter refCounter) throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.threadCount - 1;
    try (ExecutorService executorService = Executors.newFixedThreadPool(threadCount)) {
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
        final RefCounter roundRefCounter = createNewRefCounter(refCounter);
        final CountDownLatch startLatch = new CountDownLatch(threadCount);
        final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
        final long[] counts = new long[threadCount];

        for (int i = 0; i < threadCount; i++) {
          final int threadIdx = i;
          futures[threadIdx] =
              CompletableFuture.runAsync(
                  () -> {
                    // Wait for all threads to be ready
                    TestUtils.countDownThenAwait(startLatch);
                    //        System.out.println(Thread.currentThread() + " - Starting");

                    for (int j = 0; j < iterations; j++) {
                      if (abortThreads.get()) {
                        break;
                      }
                      final RefCounter.RefCounterReleaser releaser;
                      try {
                        releaser = roundRefCounter.acquire();
                        counts[threadIdx]++;
                      } catch (Env.AlreadyClosedException e) {
                        //              System.out.println(Thread.currentThread() + ", round: " +
                        // round + ", Env closed, aborting");
                        break;
                      }
                      try {
                        // Make the work between acquire and release take some time
                        TestUtils.sleep(random.nextInt(5));
                        // env is null after closure
                        Objects.requireNonNull(mockEnv.get(), "Attempt to use a null env");
                      } finally {
                        releaser.release();
                      }
                      // Random sleep after releasing so there is a time when the thread
                      // is not using the 'env'
                      TestUtils.sleep(5 + random.nextInt(5));
                    }
                    //        System.out.println(Thread.currentThread() + " - Done");
                  },
                  executorService);
        }

        // Wait for all threads to start using the ref counter
        startLatch.await();

        // Give the other threads a chance to get underway
        TestUtils.sleep(100 + random.nextInt(200));

        for (int i = 0; i < 10; i++) {
          try {
            System.out.println("count: " + roundRefCounter.getCount());
          } catch (Env.EnvInUseException e) {
            TestUtils.sleep(100 + random.nextInt(200));
          }
        }
        abortThreads.set(true);
        // Wait for all workers to finish
        CompletableFuture.allOf(futures).join();

        System.out.println("Acquire call count: " + Arrays.stream(counts).sum());

        if (roundRefCounter.getCount() != 0) {
          throw new IllegalStateException("Ref count is " + roundRefCounter.getCount());
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void immediateClose(final RefCounter refCounter) {
    final AtomicInteger onCloseCallCount = new AtomicInteger();
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(refCounter.getCount()).isZero();
    assertThat(onCloseCallCount.get()).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void failedOnCloseDoesNotCloseOrCorruptCounter() {
    final StripedRefCounter refCounter = new StripedRefCounter();

    assertThatThrownBy(
        () ->
            refCounter.close(
                () -> {
                  throw new RuntimeException("boom");
                }))
        .isInstanceOf(RuntimeException.class);

    assertThat(refCounter.isClosed()).isFalse();

    final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
    assertThat(refCounter.getCount()).isEqualTo(1);
    releaser.release();
    assertThat(refCounter.getCount()).isEqualTo(0);
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void concurrentCloseIsIdempotent() {
    final StripedRefCounter refCounter = new StripedRefCounter();
    final AtomicInteger onCloseCallCount = new AtomicInteger();

    final CountDownLatch startLatch = new CountDownLatch(2);

    final CompletableFuture<Void> first =
        CompletableFuture.runAsync(
            () -> {
              TestUtils.countDownThenAwait(startLatch);
              refCounter.close(onCloseCallCount::incrementAndGet);
            });
    final CompletableFuture<Void> second =
        CompletableFuture.runAsync(
            () -> {
              TestUtils.countDownThenAwait(startLatch);
              refCounter.close(onCloseCallCount::incrementAndGet);
            });

    CompletableFuture.allOf(first, second).join();

    assertThat(onCloseCallCount).hasValue(1);
    assertThat(refCounter.isClosed()).isTrue();
    assertThatThrownBy(refCounter::acquire).isInstanceOf(Env.AlreadyClosedException.class);
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
    final int iterationsPerThread = iterations / threadCount;
    for (int i = 0; i < threadCount; i++) {
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                // Wait for all threads to be ready
                TestUtils.countDownThenAwait(startLatch);

                // Capture the start time
                startTime.updateAndGet(
                    currVal -> {
                      if (currVal == null) {
                        return Instant.now();
                      } else {
                        return currVal;
                      }
                    });

                for (int j = 0; j < iterationsPerThread; j++) {
                  final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
                  try {
                    // Make sure we have an env that is not 'closed'
                    Objects.requireNonNull(env);
                  } finally {
                    releaser.release();
                  }
                }
                //        System.out.println(Thread.currentThread() + " - Done");
              },
              executorService);
    }
    CompletableFuture.allOf(futures).join();

    final Duration duration = Duration.between(startTime.get(), Instant.now());
    final long iterationsPerSec = Math.round((double) iterations / duration.toMillis() * 1000);

    System.out.println(
        "All Finished"
            + ", threads: "
            + threadCount
            + ", iterationsPerThread: "
            + iterationsPerThread
            + ", duration: "
            + duration
            + ", iterationsPerSec: "
            + NumberFormat.getInstance().format(iterationsPerSec));
    executorService.close();
  }

  private void runPerfTest(int stripes, final RefCounter refCounter) {
    runPerfTest(stripes, threadCount, refCounter);
  }

  private void runPerfTest(int stripes, final int threadCount, final RefCounter refCounter) {
    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final int iterationsPerThread = iterations / threadCount;
    for (int i = 0; i < threadCount; i++) {
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                // Wait for all threads to be ready
                TestUtils.countDownThenAwait(startLatch);
                // Capture the start time
                startTime.updateAndGet(
                    currVal -> {
                      if (currVal == null) {
                        return Instant.now();
                      } else {
                        return currVal;
                      }
                    });

                for (int j = 0; j < iterationsPerThread; j++) {
                  final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
                  releaser.release();
                }
              },
              executorService);
    }
    CompletableFuture.allOf(futures).join();

    if (refCounter.getCount() != 0) {
      throw new IllegalStateException("Ref count is " + refCounter.getCount());
    }

    final Duration duration = Duration.between(startTime.get(), Instant.now());
    final long iterationsPerSec = Math.round((double) iterations / duration.toMillis() * 1000);

    System.out.println(
        "All Finished"
            + ", stripes: "
            + stripes
            + ", threads: "
            + threadCount
            + ", iterationsPerThread: "
            + iterationsPerThread
            + ", duration: "
            + duration
            + ", iterationsPerSec: "
            + NumberFormat.getInstance().format(iterationsPerSec));
  }

  private static RefCounter createNewRefCounter(RefCounter refCounter) {
    // Assumes all RefCounters have a no-arg constructor
    try {
      return refCounter.getClass().getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
