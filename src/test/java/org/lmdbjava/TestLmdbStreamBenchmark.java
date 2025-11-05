package org.lmdbjava;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lmdbjava.CursorIterable.KeyVal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jakewharton.byteunits.BinaryByteUnit.GIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLmdbStreamBenchmark {

  private Path dbDir = null;
  private Env<ByteBuffer> env;
  private Dbi<ByteBuffer> dbi;

  private final int rounds = 1;
  private final int iterations = 1;
  private final int min1 = 0;
  private final int min2 = 0;
  private final int max1 = 99999;
  private final int max2 = 99;
  private final int low1 = 42;
  private final int low2 = 42;
  private final int high1 = max1 - 9999;
  private final int high2 = 42;
  private final int total1 = max1 - min1 + 1;
  private final int total2 = max2 - min2 + 1;
  private final long totalRows = total1 * total2;

  private final Point minPoint = new Point(min1, min2);
  private final Point maxPoint = new Point(max1, max2);
  private final Point lowPoint = new Point(low1, low2);
  private final Point highPoint = new Point(high1, high2);

  // Used to perform profiling.
  @Test
  void testForwardRange() throws IOException {
    dbDir = Files.createTempDirectory("lmdb");
    env = Env.create()
            .setMapSize(GIBIBYTES.toBytes(100))
            .open(dbDir.toFile());

    dbi = env.openDbi("test".getBytes(StandardCharsets.UTF_8),
            ByteBufferProxy.AbstractByteBufferProxy::compareBuff,
            DbiFlags.MDB_CREATE);
    final ByteBuffer key = ByteBuffer.allocateDirect(Integer.BYTES + Integer.BYTES);
    final ByteBuffer value = ByteBuffer.allocateDirect(0);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      for (int i = min1; i <= max1; i++) {
        for (int j = min2; j <= max2; j++) {
          key.putInt(i);
          key.putInt(j);
          key.flip();
          dbi.put(txn, key, value);
        }
      }
      txn.commit();
    }

    final Map<String, Map<Column, List<Double>>> results = new HashMap<>();
    for (int i = 0; i < rounds; i++) {
      runTimedTest(results,
              "Test forward range", Column.NEW_STREAM,
              () -> {
                testNewStream(KeyRange.builder(ByteBuffer.class)
                                .start(lowPoint.toBuffer())
                                .stop(highPoint.toBuffer())
                                .build(),
                        diff(lowPoint, highPoint) + 1,
                        createTestBuffer(low1, low2),
                        createTestBuffer(high1, low2));
              });
    }

    reportResults(results);
  }

  private void reportResults(final Map<String, Map<Column, List<Double>>> results) {
    System.out.println("|Name|Extant|New Iterator|New Stream|");
    results.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
      final Map<Column, List<Double>> values = entry.getValue();

      System.out.println("|" + entry.getKey() +
              "|" + getColumn(values, Column.EXTANT) +
              "|" + getColumn(values, Column.NEW_ITERATOR) +
              "|" + getColumn(values, Column.NEW_STREAM) +
              "|");
    });
  }

  private String getColumn(final Map<Column, List<Double>> values, final Column column) {
    final List<Double> list = values.get(column);
    if (list == null || list.isEmpty()) {
      return "";
    }
    return String.valueOf((long) removeOutliersZScore(list, 2.0)
            .stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0.0));
  }

  @Test
  void testIterationMethods() throws IOException {
//        System.setProperty(Env.DISABLE_CHECKS_PROP, "true");
    dbDir = Files.createTempDirectory("lmdb");
    env = Env.create()
            .setMapSize(GIBIBYTES.toBytes(10))
            .open(dbDir.toFile());

    dbi = env.openDbi("test".getBytes(StandardCharsets.UTF_8),
            ByteBufferProxy.AbstractByteBufferProxy::compareBuff,
            DbiFlags.MDB_CREATE);
    writeData();

    final Map<String, Map<Column, List<Double>>> results = new HashMap<>();

    for (int i = 0; i < rounds; i++) {
      runTimedTest(results, "Raw iterator", Column.NEW_ITERATOR, () -> {
        final AtomicInteger count = new AtomicInteger();
        try (final Txn<ByteBuffer> txn = env.txnRead()) {
          LmdbIterable.iterate(txn, dbi, (key, val) -> count.incrementAndGet());
        }
        assertThat(count.get()).isEqualTo(totalRows);
      });

      runTimedTest(results, "Raw iterator", Column.EXTANT,
              () -> {
                long count = 0;
                try (final Txn<ByteBuffer> txn = env.txnRead()) {
                  for (final KeyVal<ByteBuffer> ignored : dbi.iterate(txn)) {
                    count++;
                  }
                }
                assertThat(count).isEqualTo(totalRows);
              });

      // ALL FORWARD
      addTestSet(results,
              "all",
              KeyRange.all(),
              totalRows,
              minPoint,
              maxPoint);

      // ALL BACKWARD
      addTestSet(results,
              "allBackward",
              KeyRange.allBackward(),
              totalRows,
              maxPoint,
              minPoint);

      // AT LEAST
      addTestSet(results,
              "atLeast",
              KeyRange.atLeast(lowPoint.toBuffer()),
              diff(lowPoint, maxPoint) + 1,
              lowPoint,
              maxPoint);

      // AT LEAST BACKWARD
      addTestSet(results,
              "atLeastBackward",
              KeyRange.atLeastBackward(highPoint.toBuffer()),
              diff(minPoint, highPoint) + 1,
              highPoint,
              minPoint);

      // AT MOST
      addTestSet(results,
              "atMost",
              KeyRange.atMost(highPoint.toBuffer()),
              diff(minPoint, highPoint) + 1,
              minPoint,
              highPoint);

      // AT MOST BACKWARD
      addTestSet(results,
              "atMostBackward",
              KeyRange.atMostBackward(lowPoint.toBuffer()),
              diff(lowPoint, maxPoint) + 1,
              maxPoint,
              lowPoint);

      // FORWARD_CLOSED
      addTestSet(results,
              "closed",
              KeyRange.closed(lowPoint.toBuffer(), highPoint.toBuffer()),
              diff(lowPoint, highPoint) + 1,
              lowPoint,
              highPoint);

      // BACKWARD_CLOSED
      addTestSet(results,
              "closedBackward",
              KeyRange.closedBackward(highPoint.toBuffer(), lowPoint.toBuffer()),
              diff(lowPoint, highPoint) + 1,
              highPoint,
              lowPoint);

      // FORWARD_CLOSED_OPEN
      addTestSet(results,
              "closedOpen",
              KeyRange.closedOpen(lowPoint.toBuffer(), highPoint.toBuffer()),
              diff(lowPoint, highPoint),
              lowPoint,
              new Point(high1, low2 - 1));

      // BACKWARD_CLOSED_OPEN
      addTestSet(results,
              "closedOpenBackward",
              KeyRange.closedOpenBackward(highPoint.toBuffer(), lowPoint.toBuffer()),
              diff(lowPoint, highPoint),
              highPoint,
              new Point(low1, low2 + 1));

      // FORWARD_GREATER_THAN
      addTestSet(results,
              "greaterThan",
              KeyRange.greaterThan(lowPoint.toBuffer()),
              diff(lowPoint, maxPoint),
              new Point(low1, low2 + 1),
              maxPoint);

      // BACKWARD_GREATER_THAN
      addTestSet(results,
              "greaterThanBackward",
              KeyRange.greaterThanBackward(highPoint.toBuffer()),
              diff(minPoint, highPoint),
              new Point(high1, low2 - 1),
              minPoint);

      // FORWARD_LESS_THAN
      addTestSet(results,
              "lessThan",
              KeyRange.lessThan(highPoint.toBuffer()),
              diff(minPoint, highPoint),
              minPoint,
              new Point(high1, low2 - 1));

      // BACKWARD_LESS_THAN
      addTestSet(results,
              "lessThanBackward",
              KeyRange.lessThanBackward(lowPoint.toBuffer()),
              diff(lowPoint, maxPoint),
              maxPoint,
              new Point(low1, low2 + 1));

      // FORWARD_OPEN
      addTestSet(results,
              "open",
              KeyRange.open(lowPoint.toBuffer(), highPoint.toBuffer()),
              diff(lowPoint, highPoint) - 1,
              new Point(low1, low2 + 1),
              new Point(high1, low2 - 1));

      // BACKWARD_OPEN
      addTestSet(results,
              "openBackward",
              KeyRange.openBackward(highPoint.toBuffer(), lowPoint.toBuffer()),
              diff(lowPoint, highPoint) - 1,
              new Point(high1, low2 - 1),
              new Point(low1, low2 + 1));

      // FORWARD_OPEN_CLOSED
      addTestSet(results,
              "openClosed",
              KeyRange.openClosed(lowPoint.toBuffer(), highPoint.toBuffer()),
              diff(lowPoint, highPoint),
              new Point(low1, low2 + 1),
              highPoint);

      // BACKWARD_OPEN_CLOSED
      addTestSet(results,
              "openClosedBackward",
              KeyRange.openClosedBackward(highPoint.toBuffer(), lowPoint.toBuffer()),
              diff(lowPoint, highPoint),
              new Point(high1, low2 - 1),
              lowPoint);

      // PREFIX
      runTimedTest(results, "Prefix", Column.NEW_STREAM,
              () -> {
                final ByteBuffer prefix = createTestBuffer(high1);
                testNewStream(KeyRange.builder(ByteBuffer.class).prefix(prefix).build(),
                        (max2 + 1) - min2,
                        createTestBuffer(high1, min2),
                        createTestBuffer(high1, max2));
              });
      runTimedTest(results, "Prefix", Column.NEW_ITERATOR,
              () -> {
                final ByteBuffer prefix = createTestBuffer(high1);
                testNewIterator(KeyRange.builder(ByteBuffer.class).prefix(prefix).build(),
                        (max2 + 1) - min2,
                        createTestBuffer(high1, min2),
                        createTestBuffer(high1, max2));
              });

      runTimedTest(results,
              "Prefix reversed", Column.NEW_STREAM,
              () -> {
                final ByteBuffer prefix = createTestBuffer(high1);
                testNewStream(KeyRange.builder(ByteBuffer.class).prefix(prefix).reverse().build(),
                        (max2 + 1) - min2,
                        createTestBuffer(high1, max2),
                        createTestBuffer(high1, min2));
              });
      runTimedTest(results,
              "Prefix reversed", Column.NEW_ITERATOR,
              () -> {
                final ByteBuffer prefix = createTestBuffer(high1);
                testNewIterator(KeyRange.builder(ByteBuffer.class).prefix(prefix).reverse().build(),
                        (max2 + 1) - min2,
                        createTestBuffer(high1, max2),
                        createTestBuffer(high1, min2));
              });
    }

    reportResults(results);
  }

  private void addTestSet(final Map<String, Map<Column, List<Double>>> results,
                          final String name,
                          final KeyRange<ByteBuffer> keyRange,
                          final long expectedCount,
                          final Point expectedFirst,
                          final Point expectedLast) {
    final ByteBuffer first = expectedFirst.toBuffer();
    final ByteBuffer last = expectedLast.toBuffer();

    runTimedTest(results, name, Column.EXTANT,
            () -> testExtantIterator(keyRange, expectedCount, first, last));
    runTimedTest(results, name, Column.NEW_STREAM,
            () -> testNewStream(keyRange, expectedCount, first, last));
    runTimedTest(results, name, Column.NEW_ITERATOR,
            () -> testNewIterator(keyRange, expectedCount, first, last));
  }

  private void runTimedTest(final Map<String, Map<Column, List<Double>>> results,
                            final String name,
                            final Column column,
                            final Runnable runnable) {
    for (int i = 0; i < iterations; i++) {
      System.out.println("Starting: " + column + " " + name);
      final long start = System.currentTimeMillis();
      runnable.run();
      final long end = System.currentTimeMillis();
      final long elapsed = end - start;
      System.out.println("Finished: " + column + " " + name + " in " + elapsed + "ms");
      results.computeIfAbsent(name, k -> new HashMap<>())
              .computeIfAbsent(column, c -> new ArrayList<>())
              .add((double) elapsed);
    }
  }


  private void testExtantIterator(final KeyRange<ByteBuffer> keyRange,
                                  final long expectedCount,
                                  final ByteBuffer expectedFirst,
                                  final ByteBuffer expectedLast) {
    SoftAssertions.assertSoftly(softAssertions -> {
      final AtomicLong total = new AtomicLong();
      try (final Txn<ByteBuffer> txn = env.txnRead()) {
        for (final KeyVal<ByteBuffer> kv : dbi.iterate(txn, keyRange)) {
          final long count = total.incrementAndGet();
          if (count == 1) {
            softAssertions.assertThat(kv.key())
                    .withFailMessage(
                            "%s is not equal to %s",
                            getString(kv.key()),
                            getString(expectedFirst))
                    .isEqualTo(expectedFirst);
          }
          if (count == expectedCount) {
            softAssertions.assertThat(kv.key())
                    .withFailMessage(
                            "%s is not equal to %s",
                            getString(kv.key()),
                            getString(expectedLast))
                    .isEqualTo(expectedLast);
          }
        }
      }
      assertThat(total.get()).isEqualTo(expectedCount);
    });
  }

  private void testNewStream(final KeyRange<ByteBuffer> lmdbKeyRange,
                             final long expectedCount,
                             final ByteBuffer expectedFirst,
                             final ByteBuffer expectedLast) {
    SoftAssertions.assertSoftly(softAssertions -> {
      final AtomicLong total = new AtomicLong();
      try (final Txn<ByteBuffer> txn = env.txnRead()) {
        try (final Stream<KeyVal<ByteBuffer>> stream = dbi.stream(txn, lmdbKeyRange)) {
          stream.forEach(entry -> {
            final long count = total.incrementAndGet();
            if (count == 1) {
              softAssertions.assertThat(entry.key())
                      .withFailMessage(
                              "%s is not equal to %s",
                              getString(entry.key()),
                              getString(expectedFirst))
                      .isEqualTo(expectedFirst);
            }
            if (count == expectedCount) {
              softAssertions.assertThat(entry.key())
                      .withFailMessage(
                              "%s is not equal to %s",
                              getString(entry.key()),
                              getString(expectedLast))
                      .isEqualTo(expectedLast);
            }
          });
        }
      }
      assertThat(total.get()).isEqualTo(expectedCount);
    });
  }

  private void testNewIterator(final KeyRange<ByteBuffer> lmdbKeyRange,
                               final long expectedCount,
                               final ByteBuffer expectedFirst,
                               final ByteBuffer expectedLast) {
    SoftAssertions.assertSoftly(softAssertions -> {
      final AtomicLong total = new AtomicLong();
      try (final Txn<ByteBuffer> txn = env.txnRead()) {
        try (final LmdbIterable<ByteBuffer> iterable = dbi.newIterate(txn, lmdbKeyRange)) {
          iterable.forEach(entry -> {
            final long count = total.incrementAndGet();
            if (count == 1) {
              softAssertions.assertThat(entry.key())
                      .withFailMessage(
                              "%s is not equal to %s",
                              getString(entry.key()),
                              getString(expectedFirst))
                      .isEqualTo(expectedFirst);
            }
            if (count == expectedCount) {
              softAssertions.assertThat(entry.key())
                      .withFailMessage(
                              "%s is not equal to %s",
                              getString(entry.key()),
                              getString(expectedLast))
                      .isEqualTo(expectedLast);
            }
          });
        }
      }
      assertThat(total.get()).isEqualTo(expectedCount);
    });
  }

  private String getString(final ByteBuffer byteBuffer) {
    final ByteBuffer duplicate = byteBuffer.duplicate();
    return "[" + duplicate.getInt() + "," + duplicate.getInt() + "]";
  }

  @AfterEach
  final void teardown() {
    if (env != null) {
      env.close();
    }
    env = null;
    if (Files.isDirectory(dbDir)) {
      FileUtil.deleteDir(dbDir);
    }
  }

  private void writeData() {
    final ByteBuffer key = ByteBuffer.allocateDirect(Integer.BYTES + Integer.BYTES);
    final ByteBuffer value = ByteBuffer.allocateDirect(0);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      for (int i = min1; i <= max1; i++) {
        for (int j = min2; j <= max2; j++) {
          key.putInt(i);
          key.putInt(j);
          key.flip();
          dbi.put(txn, key, value);
        }
      }
      txn.commit();
    }
  }

  private static ByteBuffer createTestBuffer(final int i1) {
    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Integer.BYTES);
    byteBuffer.putInt(i1);
    byteBuffer.flip();
    return byteBuffer;
  }

  private static ByteBuffer createTestBuffer(final int i1, final int i2) {
    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Integer.BYTES + Integer.BYTES);
    byteBuffer.putInt(i1);
    byteBuffer.putInt(i2);
    byteBuffer.flip();
    return byteBuffer;
  }

  private long diff(final Point a, final Point b) {
    final long diffX = b.x > a.x
            ? b.x - a.x
            : a.x - b.x;
    final long diffY = b.y > a.y
            ? b.y - a.y
            : a.y - b.y;
    return (diffX * total2) + diffY;
  }

  private static class Point {
    final int x;
    final int y;

    public Point(int x, int y) {
      this.x = x;
      this.y = y;
    }

    ByteBuffer toBuffer() {
      return createTestBuffer(x, y);
    }
  }

  public static List<Double> removeOutliersZScore(List<Double> numbers, double threshold) {
    if (numbers.size() < 2) {
      return new ArrayList<>(numbers);
    }

    // Calculate mean
    double mean = numbers.stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0.0);

    // Calculate standard deviation
    double stdDev = calculateStandardDeviation(numbers, mean);

    // If standard deviation is 0, all values are the same
    if (stdDev == 0) {
      return new ArrayList<>(numbers);
    }

    // Filter out outliers based on z-score
    return numbers.stream()
            .filter(x -> Math.abs((x - mean) / stdDev) <= threshold)
            .collect(Collectors.toList());
  }

  private static double calculateStandardDeviation(List<Double> numbers, double mean) {
    double sumSquaredDiff = numbers.stream()
            .mapToDouble(x -> Math.pow(x - mean, 2))
            .sum();
    return Math.sqrt(sumSquaredDiff / numbers.size());
  }

  private enum Column {
    EXTANT, NEW_ITERATOR, NEW_STREAM;
  }
}
