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
  private final int processorCount = PROCESSOR_COUNT;

  /**
   * @return A {@link Stream} of all {@link RefCounter}s for {@link ParameterizedTest}s.
   */
  private static Stream<Arguments> allRefCounterProvider() {
    return Stream.concat(
        multiThreadedRefCounterProvider(),
        Stream.of(new SingleThreadedRefCounter(), new NoOpRefCounter())
            .map(RefCounterTest::createArguments));
  }

  /**
   * @return A {@link Stream} of {@link RefCounter}s that support multithreaded use for {@link
   *     ParameterizedTest}s.
   */
  private static Stream<Arguments> multiThreadedRefCounterProvider() {
    return Stream.of(new StripedRefCounter(), new SimpleRefCounter(), new SynchronisedRefCounter())
        .map(RefCounterTest::createArguments);
  }

  private static Arguments createArguments(final RefCounter refCounter) {
    return Arguments.argumentSet(refCounter.getClass().getSimpleName(), refCounter);
  }

  @Disabled // Manual performance test
  @Test
  public void perfTest() {
    // Do multiple rounds to let it warm up
    for (int i = 1; i <= 3; i++) {
      final int round = i;
      // Run tests with all available processors
      System.out.println(
          "Multi-threaded ("
              + processorCount
              + " threads) tests ---------------------------------");

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

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void testRefCounters_close(final RefCounter refCounter) {
    // Acquire twice
    final RefCounter.RefCounterReleaser releaser1 = refCounter.acquire();
    assertRefCount(refCounter, 1);
    final RefCounter.RefCounterReleaser releaser2 = refCounter.acquire();
    assertRefCount(refCounter, 2);

    final AtomicInteger onCloseCallCount = new AtomicInteger();

    if (!(refCounter instanceof NoOpRefCounter)) {
      // Close() not called as ref count is two.

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
      // Close() not called as ref count is one.
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
  @MethodSource("allRefCounterProvider")
  void testRefCounters_tryClose(final RefCounter refCounter) {
    // Acquire twice
    final RefCounter.RefCounterReleaser releaser1 = refCounter.acquire();
    assertRefCount(refCounter, 1);
    final RefCounter.RefCounterReleaser releaser2 = refCounter.acquire();
    assertRefCount(refCounter, 2);

    final AtomicInteger onCloseCallCount = new AtomicInteger();

    if (!(refCounter instanceof NoOpRefCounter)) {
      // Close() not called as ref count is two.
      assertThat(refCounter.tryClose(onCloseCallCount::incrementAndGet)).isFalse();
    }
    assertThat(onCloseCallCount).hasValue(0);

    // Release 1st releaser
    releaser1.release();
    assertRefCount(refCounter, 1);

    if (!(refCounter instanceof NoOpRefCounter)) {
      // Close() not called as ref count is one.
      assertThat(refCounter.tryClose(onCloseCallCount::incrementAndGet)).isFalse();
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
    assertThat(refCounter.tryClose(onCloseCallCount::incrementAndGet)).isTrue();
    assertThat(onCloseCallCount).hasValue(1);

    // no-op as onClose already called
    assertThat(refCounter.tryClose(onCloseCallCount::incrementAndGet)).isFalse();
    assertThat(onCloseCallCount).hasValue(1);
  }

  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void multipleThreads(final RefCounter refCounter) {
    final int iterations = 1000;
    final AtomicInteger[] callCounts = new AtomicInteger[processorCount];
    for (int i = 0; i < processorCount; i++) {
      callCounts[i] = new AtomicInteger();
    }
    final CountDownLatch countDownLatch = new CountDownLatch(processorCount);
    //noinspection resource ExecutorService does not implement AutoCloseable in Java8
    final ExecutorService executorService = Executors.newFixedThreadPool(processorCount);
    try {

      final CompletableFuture<?>[] futures =
          IntStream.range(0, processorCount)
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
    } finally {
      // ExecutorService does not implement AutoCloseable in Java8
      executorService.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void multipleThreads_delayedRelease(final RefCounter refCounter) {
    final int iterations = 1000;
    final AtomicInteger[] callCounts;
    final Queue<RefCounter.RefCounterReleaser> releasers;

    //noinspection resource ExecutorService does not implement AutoCloseable in Java8
    final ExecutorService executorService = Executors.newFixedThreadPool(processorCount);
    final ExecutorService executorService2 = Executors.newFixedThreadPool(processorCount);

    try {
      callCounts = new AtomicInteger[processorCount];
      for (int i = 0; i < processorCount; i++) {
        callCounts[i] = new AtomicInteger();
      }
      final CountDownLatch countDownLatch = new CountDownLatch(processorCount);

      releasers = new ConcurrentLinkedQueue<>();
      final Queue<CompletableFuture<?>> futures = new ConcurrentLinkedQueue<>();

      IntStream.range(0, processorCount)
          .boxed()
          .map(
              i ->
                  CompletableFuture.runAsync(
                      () -> {
                        TestUtils.countDownThenAwait(countDownLatch);
                        for (int j = 0; j < iterations; j++) {
                          final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
                          releasers.add(releaser);
                          callCounts[i].getAndIncrement();
                          futures.add(
                              CompletableFuture.runAsync(
                                  () -> {
                                    final long count = refCounter.getCount();
                                    assertThat(count).isNotEqualTo(0);
                                  },
                                  executorService2));
                        }
                      },
                      executorService))
          .forEach(futures::add);

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } finally {
      executorService2.shutdown();
      executorService.shutdown();
    }

    assertRefCount(refCounter, processorCount * iterations);

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

    assertThatThrownBy(refCounter::checkNotClosed).isInstanceOf(Env.AlreadyClosedException.class);

    // Check again as idempotent
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount).hasValue(1);
    assertThat(refCounter.isClosed()).isEqualTo(true);

    assertThatThrownBy(refCounter::checkNotClosed).isInstanceOf(Env.AlreadyClosedException.class);
  }

  /**
   * Lots of threads all doing acquire/release in a loop, then the main thread tries to call
   * refCounter.close(...), which will throw an {@link org.lmdbjava.Env.EnvInUseException}. The main
   * thread then makes all worker threads stop their looping and calls refCounter.close(...) again,
   * successfully this time.
   */
  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void testBehaviour(final RefCounter refCounter) throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.processorCount - 1;
    //noinspection resource ExecutorService does not implement AutoCloseable in Java8
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    try {
      final int rounds = 5;
      final int iterations = 10_000_000;
      final AtomicReference<Object> mockEnv = new AtomicReference<>();

      for (int k = 0; k < rounds; k++) {

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
                    for (int j = 0; j < iterations; j++) {
                      if (abortThreads.get()) {
                        break;
                      }

                      final RefCounter.RefCounterReleaser releaser;
                      try {
                        releaser = roundRefCounter.acquire();
                        counts[threadIdx].incrementAndGet();
                      } catch (Env.AlreadyClosedException e) {
                        break;
                      }
                      try {
                        // Make the work between acquire and release take some time
                        TestUtils.sleep(random.nextInt(5));
                        // env is null after closure
                        assertThat(mockEnv.get()).isNotNull();
                        //                      Objects.requireNonNull(mockEnv.get(), "Attempt to
                        // use a null env");
                      } finally {
                        releaser.release();
                      }
                    }
                  },
                  executorService);
        }

        // Wait for all threads to start using the ref counter
        startLatch.await();

        // Give the other threads a chance to get underway
        TestUtils.sleep(200 + random.nextInt(200));
        final AtomicBoolean didClose = new AtomicBoolean(false);
        final AtomicInteger onCloseCallCount = new AtomicInteger();
        while (!didClose.get()) {
          try {
            assertThat(mockEnv.get()).isNotNull();
            roundRefCounter.close(
                () -> {
                  onCloseCallCount.incrementAndGet();
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

        // Make sure the mock env is all closed down
        assertThat(mockEnv).hasNullValue();
        assertThat(roundRefCounter.isClosed()).isEqualTo(true);
        assertThat(roundRefCounter.getCount()).isZero();
        assertThatThrownBy(roundRefCounter::acquire).isInstanceOf(Env.AlreadyClosedException.class);
        assertThat(onCloseCallCount).hasValue(1);
      }
    } finally {
      // ExecutorService does not implement AutoCloseable in Java8
      executorService.shutdown();
    }
  }

  /**
   * Ensure we can call getCount when multiple threads are all calling acquire/release in a loop.
   */
  @ParameterizedTest
  @MethodSource("multiThreadedRefCounterProvider")
  void testGetCount(final RefCounter refCounter) throws InterruptedException {
    final Random random = new Random();
    final int threadCount = this.processorCount - 1;
    //noinspection resource ExecutorService does not implement AutoCloseable in Java8
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    try {
      final int rounds = 5;
      final int iterations = 10_000_000;
      final AtomicReference<Object> mockEnv = new AtomicReference<>();
      final AtomicBoolean abortThreads = new AtomicBoolean(false);

      for (int k = 0; k < rounds; k++) {
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

                    for (int j = 0; j < iterations; j++) {
                      if (abortThreads.get()) {
                        break;
                      }
                      final RefCounter.RefCounterReleaser releaser;
                      try {
                        releaser = roundRefCounter.acquire();
                        counts[threadIdx]++;
                      } catch (Env.AlreadyClosedException e) {
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
                  },
                  executorService);
        }

        // Wait for all threads to start using the ref counter
        startLatch.await();

        // Give the other threads a chance to get underway
        TestUtils.sleep(100 + random.nextInt(200));

        for (int i = 0; i < 10; i++) {
          try {
            // Makes sure we can acquire the ref counter count
            roundRefCounter.getCount();
          } catch (Env.EnvInUseException e) {
            TestUtils.sleep(100 + random.nextInt(200));
          }
        }
        abortThreads.set(true);
        // Wait for all workers to finish
        CompletableFuture.allOf(futures).join();

        if (roundRefCounter.getCount() != 0) {
          throw new IllegalStateException("Ref count is " + roundRefCounter.getCount());
        }
      }
    } finally {
      // ExecutorService does not implement AutoCloseable in Java8
      executorService.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void immediateClose(final RefCounter refCounter) {
    final AtomicInteger onCloseCallCount = new AtomicInteger();
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(refCounter.getCount()).isZero();
    assertThat(refCounter.isClosed()).isTrue();
    assertThatThrownBy(refCounter::checkNotClosed).isInstanceOf(Env.AlreadyClosedException.class);
    assertThat(onCloseCallCount.get()).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void acquireAfterClose(final RefCounter refCounter) {
    final AtomicInteger onCloseCallCount = new AtomicInteger();
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount.get()).isEqualTo(1);

    if (!(refCounter instanceof NoOpRefCounter)) {
      assertThatThrownBy(refCounter::acquire).isInstanceOf(Env.AlreadyClosedException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void releaseAfterClose(final RefCounter refCounter) {
    final AtomicInteger onCloseCallCount = new AtomicInteger();
    final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
    // Need to release to allow the close
    releaser.release();
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount.get()).isEqualTo(1);

    // This is a no-op as already released
    releaser.release();
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void countAfterClose(final RefCounter refCounter) {
    final AtomicInteger onCloseCallCount = new AtomicInteger();
    assertThat(refCounter.getCount()).isZero();
    final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
    if (!(refCounter instanceof NoOpRefCounter)) {
      assertThat(refCounter.getCount()).isEqualTo(1);
    }
    // Need to release to allow the close
    releaser.release();
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount.get()).isEqualTo(1);

    assertThat(refCounter.getCount()).isZero();
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void use_null(final RefCounter refCounter) {
    // A no-op
    refCounter.use(null);

    final AtomicInteger onCloseCallCount = new AtomicInteger();
    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount.get()).isEqualTo(1);

    // A no-op
    refCounter.use(null);
  }

  @ParameterizedTest
  @MethodSource("allRefCounterProvider")
  void use(final RefCounter refCounter) {
    final AtomicInteger onCloseCallCount = new AtomicInteger();
    final AtomicInteger useCallCount = new AtomicInteger();
    refCounter.use(
        () -> {
          useCallCount.incrementAndGet();
          if (!(refCounter instanceof NoOpRefCounter)) {
            assertThatThrownBy(() -> refCounter.close(onCloseCallCount::incrementAndGet))
                .isInstanceOf(Env.EnvInUseException.class);
          }
        });

    refCounter.use(
        () -> {
          useCallCount.incrementAndGet();
          if (!(refCounter instanceof NoOpRefCounter)) {
            assertThatThrownBy(() -> refCounter.close(onCloseCallCount::incrementAndGet))
                .isInstanceOf(Env.EnvInUseException.class);
          }
        });

    assertThat(useCallCount.get()).isEqualTo(2);
    assertThat(onCloseCallCount.get()).isEqualTo(0);

    assertThat(refCounter.getCount()).isEqualTo(0);

    refCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount.get()).isEqualTo(1);

    // use after close
    if (!(refCounter instanceof NoOpRefCounter)) {
      assertThatThrownBy(() -> refCounter.use(useCallCount::incrementAndGet))
          .isInstanceOf(Env.AlreadyClosedException.class);
    }
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
    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[processorCount];
    final NoOpRefCounter refCounter = new NoOpRefCounter();
    final CountDownLatch startLatch = new CountDownLatch(processorCount);
    final ExecutorService executorService = Executors.newFixedThreadPool(processorCount);
    try {
      final int iterationsPerThread = iterations / processorCount;
      for (int i = 0; i < processorCount; i++) {
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
                    // Just acquire then release
                    final RefCounter.RefCounterReleaser releaser = refCounter.acquire();
                    releaser.release();
                  }
                },
                executorService);
      }
      CompletableFuture.allOf(futures).join();

      //      final Duration duration = Duration.between(startTime.get(), Instant.now());
      //      final long iterationsPerSec = Math.round((double) iterations / duration.toMillis() *
      // 1000);
      //      System.out.println(
      //          "All Finished"
      //              + ", threads: "
      //              + threadCount
      //              + ", iterationsPerThread: "
      //              + iterationsPerThread
      //              + ", duration: "
      //              + duration
      //              + ", iterationsPerSec: "
      //              + NumberFormat.getInstance().format(iterationsPerSec));
    } finally {
      // ExecutorService does not implement AutoCloseable in Java8
      executorService.shutdown();
    }
  }

  private void runPerfTest(int stripes, final RefCounter refCounter) {
    runPerfTest(stripes, processorCount, refCounter);
  }

  private void runPerfTest(int stripes, final int threadCount, final RefCounter refCounter) {
    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    try {
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
    } finally {
      // ExecutorService does not implement AutoCloseable in Java8
      executorService.shutdown();
    }
  }

  private static RefCounter createNewRefCounter(RefCounter refCounter) {
    // Assumes all RefCounters have a no-arg constructor
    try {
      return refCounter.getClass().getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void assertRefCount(final RefCounter refCounter, final int expectedCount) {
    // NoOpRefCounter does no reference counting, so we can't assert the count
    if (!(refCounter instanceof NoOpRefCounter)) {
      assertThat(refCounter.getCount()).isEqualTo(expectedCount);
    }
  }
}
