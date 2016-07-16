/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * A {@code BinaryByteUnit} represents power-of-two byte sizes at a given unit
 * of granularity and provides utility methods to convert across units. A
 * {@code BinaryByteUnit} does not maintain byte size information, but only
 * helps organize and use byte size representations that may be maintained
 * separately across various contexts.
 *
 * @author Jake Wharton
 */
public enum ByteUnit {

  /**
   * Byte unit representing one byte.
   */
  BYTES {
    @Override public long convert(final long sourceCount,
                                  final ByteUnit sourceUnit) {
      return sourceUnit.toBytes(sourceCount);
    }

    @Override public long toBytes(final long count) {
      return count;
    }

    @Override public long toKibibytes(final long count) {
      return count / (KB / B);
    }

    @Override public long toMebibytes(final long count) {
      return count / (MB / B);
    }

    @Override public long toGibibytes(final long count) {
      return count / (GB / B);
    }

    @Override public long toTebibytes(final long count) {
      return count / (TB / B);
    }

    @Override public long toPebibytes(final long count) {
      return count / (PB / B);
    }
  },
  /**
   * A byte unit representing 1024 bytes.
   */
  KIBIBYTES {
    @Override public long convert(final long sourceCount,
                                  final ByteUnit sourceUnit) {
      return sourceUnit.toKibibytes(sourceCount);
    }

    @Override public long toBytes(final long count) {
      return multiply(count, KB / B, MAX / (KB / B));
    }

    @Override public long toKibibytes(final long count) {
      return count;
    }

    @Override public long toMebibytes(final long count) {
      return count / (MB / KB);
    }

    @Override public long toGibibytes(final long count) {
      return count / (GB / KB);
    }

    @Override public long toTebibytes(final long count) {
      return count / (TB / KB);
    }

    @Override public long toPebibytes(final long count) {
      return count / (PB / KB);
    }
  },
  /**
   * A byte unit representing 1024 kibibytes.
   */
  MEBIBYTES {
    @Override public long convert(final long sourceCount,
                                  final ByteUnit sourceUnit) {
      return sourceUnit.toMebibytes(sourceCount);
    }

    @Override public long toBytes(final long count) {
      return multiply(count, MB / B, MAX / (MB / B));
    }

    @Override public long toKibibytes(final long count) {
      return multiply(count, MB / KB, MAX / (MB / KB));
    }

    @Override public long toMebibytes(final long count) {
      return count;
    }

    @Override public long toGibibytes(final long count) {
      return count / (GB / MB);
    }

    @Override public long toTebibytes(final long count) {
      return count / (TB / MB);
    }

    @Override public long toPebibytes(final long count) {
      return count / (PB / MB);
    }
  },
  /**
   * A byte unit representing 1024 mebibytes.
   */
  GIBIBYTES {
    @Override public long convert(final long sourceCount,
                                  final ByteUnit sourceUnit) {
      return sourceUnit.toGibibytes(sourceCount);
    }

    @Override public long toBytes(final long count) {
      return multiply(count, GB / B, MAX / (GB / B));
    }

    @Override public long toKibibytes(final long count) {
      return multiply(count, GB / KB, MAX / (GB / KB));
    }

    @Override public long toMebibytes(final long count) {
      return multiply(count, GB / MB, MAX / (GB / MB));
    }

    @Override public long toGibibytes(final long count) {
      return count;
    }

    @Override public long toTebibytes(final long count) {
      return count / (TB / GB);
    }

    @Override public long toPebibytes(final long count) {
      return count / (PB / GB);
    }
  },
  /**
   * A byte unit representing 1024 gibibytes.
   */
  TEBIBYTES {
    @Override public long convert(final long sourceCount,
                                  final ByteUnit sourceUnit) {
      return sourceUnit.toTebibytes(sourceCount);
    }

    @Override public long toBytes(final long count) {
      return multiply(count, TB / B, MAX / (TB / B));
    }

    @Override public long toKibibytes(final long count) {
      return multiply(count, TB / KB, MAX / (TB / KB));
    }

    @Override public long toMebibytes(final long count) {
      return multiply(count, TB / MB, MAX / (TB / MB));
    }

    @Override public long toGibibytes(final long count) {
      return multiply(count, TB / GB, MAX / (TB / GB));
    }

    @Override public long toTebibytes(final long count) {
      return count;
    }

    @Override public long toPebibytes(final long count) {
      return count / (PB / TB);
    }
  },
  /**
   * A byte unit representing 1024 tebibytes.
   */
  PEBIBYTES {
    @Override public long convert(final long sourceCount,
                                  final ByteUnit sourceUnit) {
      return sourceUnit.toPebibytes(sourceCount);
    }

    @Override public long toBytes(final long count) {
      return multiply(count, PB / B, MAX / (PB / B));
    }

    @Override public long toKibibytes(final long count) {
      return multiply(count, PB / KB, MAX / (PB / KB));
    }

    @Override public long toMebibytes(final long count) {
      return multiply(count, PB / MB, MAX / (PB / MB));
    }

    @Override public long toGibibytes(final long count) {
      return multiply(count, PB / GB, MAX / (PB / GB));
    }

    @Override public long toTebibytes(final long count) {
      return multiply(count, PB / TB, MAX / (PB / TB));
    }

    @Override public long toPebibytes(final long count) {
      return count;
    }
  };

  static final String DEFAULT_FORMAT_PATTERN = "#,##0.#";
  private static final long B = 1L;
  private static final long KB = B * 1_024L;
  private static final long MAX = Long.MAX_VALUE;
  private static final long MB = KB * 1_024L;
  private static final long GB = MB * 1_024L;
  private static final long TB = GB * 1_024L;
  private static final long PB = TB * 1_024L;
  private static final String[] UNITS = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};

  /**
   * Return {@code bytes} as human-readable size string (e.g., "1.2 GiB". This
   * will use a default {@link DecimalFormat} instance for formatting the
   * number.
   *
   * @param bytes number of bytes
   * @return a human-readable size string
   */
  public static String format(final long bytes) {
    return format(bytes, new DecimalFormat(DEFAULT_FORMAT_PATTERN));
  }

  /**
   * Return {@code bytes} as human-readable size string (e.g., "1.2 GiB". This
   * will use a {@link DecimalFormat} instance with {@code pattern} for
   * formatting the number.
   *
   * @param bytes   number of bytes
   * @param pattern decimal format pattern
   * @return a human-readable size string
   */
  public static String format(final long bytes, final String pattern) {
    return format(bytes, new DecimalFormat(pattern));
  }

  /**
   * Return {@code bytes} as human-readable size string (e.g., "1.2 GiB". This
   * will use {@code
   * format} for formatting the number.
   *
   * @param bytes  number of bytes
   * @param format number format object
   * @return a human-readable size string
   */
  public static String format(final long bytes, final NumberFormat format) {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes < 0: " + bytes);
    }

    int unitIndex = 0;
    double count = bytes;
    while (count >= 1024d && unitIndex < UNITS.length - 1) {
      count /= 1024d;
      unitIndex += 1;
    }
    return format.format(count) + ' ' + UNITS[unitIndex];
  }

  @SuppressWarnings("checkstyle:returncount")
  private static long multiply(final long size, final long factor,
                               final long over) {
    if (size > over) {
      return Long.MAX_VALUE;
    }
    if (size < -over) {
      return Long.MIN_VALUE;
    }
    return size * factor;
  }

  /**
   * Converts the given size in the given unit to this unit. Conversions from
   * finer to coarser granularities truncate, so lose precision. For example,
   * converting from {@code 999} bytes to kibibytes results in {@code 0}.
   * Conversions from coarser to finer granularities with arguments that would
   * numerically overflow saturate to {@code Long.MIN_VALUE} if negative or
   * {@code Long.MAX_VALUE} if positive.
   *
   * <p>
   * For example, to convert 10 kilobytes to bytes, use:
   * {@code ByteUnit.KIBIBYTES.convert(10, ByteUnit.BYTES)}
   *
   * @param sourceCount the size in the given {@code sourceUnit}.
   * @param sourceUnit  the unit of the {@code sourceCount} argument.
   * @return the converted size in this unit, or {@code Long.MIN_VALUE} if
   *         conversion would negatively overflow, or {@code Long.MAX_VALUE} if
   *         it would positively overflow.
   */
  public long convert(final long sourceCount, final ByteUnit sourceUnit) {
    throw new AbstractMethodError();
  }

  /**
   * Converts the given size in the given unit to bytes. Conversions with
   * arguments that would numerically overflow saturate to
   * {@code Long.MIN_VALUE} if negative or {@code Long.MAX_VALUE} if positive.
   *
   * @param count the bit count
   * @return the converted count, or {@code Long.MIN_VALUE} if conversion would
   *         negatively overflow, or {@code Long.MAX_VALUE} if it would
   *         positively overflow.
   */
  public long toBytes(final long count) {
    throw new AbstractMethodError();
  }

  /**
   * Equivalent to
   * {@link #convert(long, ByteUnit) GIBIBYTES.convert(count, this)}.
   *
   * @param count the bit count
   * @return the converted count, or {@code Long.MIN_VALUE} if conversion would
   *         negatively overflow, or {@code Long.MAX_VALUE} if it would
   *         positively overflow.
   */
  public long toGibibytes(final long count) {
    throw new AbstractMethodError();
  }

  /**
   * Equivalent to
   * {@link #convert(long, ByteUnit) KIBIBYTES.convert(count, this)}.
   *
   * @param count the bit count
   * @return the converted count, or {@code Long.MIN_VALUE} if conversion would
   *         negatively overflow, or {@code Long.MAX_VALUE} if it would
   *         positively overflow.
   */
  public long toKibibytes(final long count) {
    throw new AbstractMethodError();
  }

  /**
   * Equivalent to
   * {@link #convert(long, ByteUnit) MEBIBYTES.convert(count, this)}.
   *
   * @param count the bit count
   * @return the converted count, or {@code Long.MIN_VALUE} if conversion would
   *         negatively overflow, or {@code Long.MAX_VALUE} if it would
   *         positively overflow.
   */
  public long toMebibytes(final long count) {
    throw new AbstractMethodError();
  }

  /**
   * Equivalent to
   * {@link #convert(long, ByteUnit) PEBIBYTES.convert(count, this)}.
   *
   * @param count the bit count
   * @return the converted count, or {@code Long.MIN_VALUE} if conversion would
   *         negatively overflow, or {@code Long.MAX_VALUE} if it would
   *         positively overflow.
   */
  public long toPebibytes(final long count) {
    throw new AbstractMethodError();
  }

  /**
   * Equivalent to
   * {@link #convert(long, ByteUnit) TEBIBYTES.convert(count, this)}.
   *
   * @param count the bit count
   * @return the converted count, or {@code Long.MIN_VALUE} if conversion would
   *         negatively overflow, or {@code Long.MAX_VALUE} if it would
   *         positively overflow.
   */
  public long toTebibytes(final long count) {
    throw new AbstractMethodError();
  }

}
