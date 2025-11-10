/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
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

/** Simple {@link Enum} for converting various IEC and SI byte units down a number of bytes. */
public enum ByteUnit {

  /** IEC/SI byte unit for bytes. */
  BYTES(1L),

  /** IEC byte unit for 1024 bytes. */
  KIBIBYTES(1_024L),
  /** IEC byte unit for 1024^2 bytes. */
  MEBIBYTES(1_048_576L),
  /** IEC byte unit for 1024^3 bytes. */
  GIBIBYTES(1_073_741_824L),
  /** IEC byte unit for 1024^4 bytes. */
  TEBIBYTES(1_099_511_627_776L),
  /** IEC byte unit for 1024^5 bytes. */
  PEBIBYTES(1_125_899_906_842_624L),

  /** SI byte unit for 1000 bytes. */
  KILOBYTES(1_000L),
  /** SI byte unit for 1000^2 bytes. */
  MEGABYTES(1_000_000L),
  /** SI byte unit for 1000^3 bytes. */
  GIGABYTES(1_000_000_000L),
  /** SI byte unit for 1000^4 bytes. */
  TERABYTES(1_000_000_000_000L),
  /** SI byte unit for 1000^5 bytes. */
  PETABYTES(1_000_000_000_000_000L),
  ;

  private final long factor;

  ByteUnit(long factor) {
    this.factor = factor;
  }

  /**
   * Convert the value in this byte unit into bytes.
   *
   * @param value The value to convert.
   * @return The number of bytes.
   */
  public long toBytes(final long value) {
    return value * factor;
  }

  /**
   * Gets factor to apply when converting this unit into bytes.
   *
   * @return The factor to apply when converting this unit into bytes.
   */
  public long getFactor() {
    return factor;
  }
}
