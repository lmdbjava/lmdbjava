package org.lmdbjava;


import org.jspecify.annotations.NonNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

public class RefCounterBenchmark {

  private static final int ITERATIONS = 2;
  private static final int WARMUP = 2;
  private static final int FORK = 2;

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @Measurement(iterations = ITERATIONS)
  @Warmup(iterations = WARMUP)
  @Fork(value = FORK, warmups = WARMUP)
  @Threads(Threads.MAX)
  public void allThreads(final MultiThreadPlan plan, final Blackhole blackhole) {
    final RefCounter.RefCounterReleaser releaser = plan.refCounter.acquire();
    blackhole.consume(releaser);
    releaser.release();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @Measurement(iterations = ITERATIONS)
  @Warmup(iterations = WARMUP)
  @Fork(value = FORK, warmups = WARMUP)
  @Threads(8)
  public void eightThreads(final MultiThreadPlan plan, final Blackhole blackhole) {
    final RefCounter.RefCounterReleaser releaser = plan.refCounter.acquire();
    blackhole.consume(releaser);
    releaser.release();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @Measurement(iterations = ITERATIONS)
  @Warmup(iterations = WARMUP)
  @Fork(value = FORK, warmups = WARMUP)
  @Threads(4)
  public void fourThreads(final MultiThreadPlan plan, final Blackhole blackhole) {
    final RefCounter.RefCounterReleaser releaser = plan.refCounter.acquire();
    blackhole.consume(releaser);
    releaser.release();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @Measurement(iterations = ITERATIONS)
  @Warmup(iterations = WARMUP)
  @Fork(value = FORK, warmups = WARMUP)
  @Threads(2)
  public void twoThreads(final MultiThreadPlan plan, final Blackhole blackhole) {
    final RefCounter.RefCounterReleaser releaser = plan.refCounter.acquire();
    blackhole.consume(releaser);
    releaser.release();
  }


  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @Measurement(iterations = ITERATIONS)
  @Warmup(iterations = WARMUP)
  @Fork(value = FORK, warmups = WARMUP)
  @Threads(1)
  public void oneThread(final SingleThreadPlan plan, final Blackhole blackhole) {
    final RefCounter.RefCounterReleaser releaser = plan.refCounter.acquire();
    blackhole.consume(releaser);
    releaser.release();
  }

  private static @NonNull RefCounter getRefCounter(final String refCounterName) {
    final RefCounter refCounter;
    switch (refCounterName) {
      case "striped":
        refCounter = new StripedRefCounter();
        break;
      case "simple":
        refCounter = new SimpleRefCounter();
        break;
      case "synchronised":
        refCounter = new SynchronisedRefCounter();
        break;
      case "no-op":
        refCounter = new NoOpRefCounter();
        break;
      case "single":
        refCounter = new SingleThreadedRefCounter();
        break;
      default:
        throw new IllegalArgumentException("Unknown name '" + refCounterName + "'");
    }
    return refCounter;
  }

  @State(Scope.Benchmark)
  public static class MultiThreadPlan {

    private RefCounter refCounter;

    @Param({"striped", "simple", "synchronised", "no-op"})
    public String refCounterName;

    @Setup(Level.Invocation)
    public void setUp() {
      this.refCounter = getRefCounter(refCounterName);
    }
  }

  @State(Scope.Benchmark)
  public static class SingleThreadPlan {

    private RefCounter refCounter;

    @Param({"striped", "simple", "synchronised", "no-op", "single"})
    public String refCounterName;

    @Setup(Level.Invocation)
    public void setUp() {
      this.refCounter = getRefCounter(refCounterName);
    }
  }
}
