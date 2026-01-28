package org.lmdbjava;


import static org.assertj.core.api.Assertions.assertThat;

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
    // Do multiple rounds to let it warm up
    for (int i = 1; i <= 3; i++) {
      System.out.println("Round: " + i + " StripedRefCounterImpl");

      IntStream.of(1, 2, 4, 8, threadCount, threadCount * 2)
          .forEach(stripes -> runTest(stripes, new StripedRefCounterImpl(stripes, this::onClose)));

      System.out.println("Round: " + i + " StripedCounter");
      IntStream.of(1, 2, 4, 8, threadCount, threadCount * 2)
          .forEach(stripes -> runTest(stripes, new StampedLockRefCounterImpl(stripes)));
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
    final NoOpRefCounter refCounter = new NoOpRefCounter(this::onClose);
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
//    System.out.println("Running test for " + stripes + " stripes");

    final AtomicReference<Instant> startTime = new AtomicReference<>(null);
    final CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
//    final RefCounter refCounter = new StripedRefCounterImpl(stripes, this::onClose);
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

      if (refCounter.getCount() != 0) {
        throw new IllegalStateException("Ref count is " + refCounter.getCount());
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

  @Test
  void testBits() {
    long val = 0;
    val = val | (1L << 0);
    val = val | (1L << 3);
    val = val | (1L << 63);

    System.out.println("val: " + val + ", bits: " + Long.toBinaryString(val));

    for (int i = 0; i < 64; i++) {
      System.out.println("i: " + i + ", bit: " + getBit(val, i));
    }
  }

  @Test
  void testAtomicBitSet() {
    final int size = 16;
    final AtomicBitSet bitSet = new AtomicBitSet(16);

    assertThat(bitSet.isSet(3))
        .isEqualTo(false);
    assertThat(bitSet.countSet())
        .isEqualTo(0);
    assertThat(bitSet.flip(3))
        .isEqualTo(true);
    assertThat(bitSet.countSet())
        .isEqualTo(1);
    assertThat(bitSet.flip(3))
        .isEqualTo(false);
    assertThat(bitSet.countSet())
        .isEqualTo(0);

    bitSet.setAndGet(3);
    bitSet.setAndGet(10);
    assertThat(bitSet.countSet())
        .isEqualTo(2);
    bitSet.setAndGet(10);
    assertThat(bitSet.countSet())
        .isEqualTo(2);
    bitSet.unset(10);
    assertThat(bitSet.countSet())
        .isEqualTo(1);
    bitSet.unset(10);
    assertThat(bitSet.countSet())
        .isEqualTo(1);

    bitSet.unSetAll();
    assertThat(bitSet.countSet())
        .isEqualTo(0);

    System.out.println("val: " + bitSet.asLong() + ", bits: " + bitSet);
    for (int i = 0; i < size; i++) {
      bitSet.setAndGet(i);
    }
    System.out.println("val: " + bitSet.asLong() + ", bits: " + bitSet);

    bitSet.setAll();
    assertThat(bitSet.countSet())
        .isEqualTo(size);
    System.out.println("val: " + bitSet.asLong() + ", bits: " + bitSet);

    bitSet.unSetAll();
    long asLong = bitSet.asLong();
    assertThat(bitSet.getAndSet(5))
        .isEqualTo(asLong);
    assertThat(bitSet.getAndSet(5))
        .isNotEqualTo(asLong);

    bitSet.unSetAll();
    assertThat(bitSet.countSet())
        .isEqualTo(0);
    assertThat(bitSet.countUnSet())
        .isEqualTo(size);

    bitSet.unSetAll();
    bitSet.setAndGet(3);
    bitSet.setAndGet(10);
    assertThat(bitSet.countSet())
        .isEqualTo(2);
    assertThat(bitSet.countUnSet())
        .isEqualTo(size - 2);
  }

  private String longAsBits(long val) {
    return Long.toBinaryString(val);
  }

  private long getBit(final long val, final long idx) {
    return (val >> idx) & 1;
  }


}
