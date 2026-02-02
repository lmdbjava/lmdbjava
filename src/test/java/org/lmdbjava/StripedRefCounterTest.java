package org.lmdbjava;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class StripedRefCounterTest {

  @Test
  void acquire() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    final RefCounter.RefCounterReleaser releaser = stripedRefCounter.acquire();

    assertThat(stripedRefCounter.getCount())
        .isEqualTo(1);

    final AtomicInteger onCloseCallCount = new AtomicInteger();

    Assertions.assertThatThrownBy(
            () -> {
              stripedRefCounter.close(onCloseCallCount::incrementAndGet);
            })
        .isInstanceOf(Env.EnvInUseException.class);
    assertThat(onCloseCallCount)
        .hasValue(0);

    releaser.release();

    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);

    releaser.release();

    assertThat(stripedRefCounter.getCount())
        .isEqualTo(0);

    stripedRefCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount)
        .hasValue(1);

    // Idempotent
    stripedRefCounter.close(onCloseCallCount::incrementAndGet);
    assertThat(onCloseCallCount)
        .hasValue(1);
  }

  @Test
  void multipleThreads() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    final int threads = Runtime.getRuntime().availableProcessors();
    final int iterations = 100;
    final AtomicInteger[] callCounts = new AtomicInteger[threads];
    for (int i = 0; i < threads; i++) {
      callCounts[i] = new AtomicInteger();
    }

    IntStream.range(0, threads)
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
}
