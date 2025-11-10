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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ByteUnitTest {

  @Test
  void test() {
    Assertions.assertThat(ByteUnit.BYTES.toBytes(2)).isEqualTo(2);

    // BYTES
    Assertions.assertThat(ByteUnit.BYTES.toBytes(2)).isEqualTo(2L);
    Assertions.assertThat(ByteUnit.BYTES.toBytes(0)).isEqualTo(0L);
    Assertions.assertThat(ByteUnit.BYTES.getFactor()).isEqualTo(1L);

    // IEC Units
    Assertions.assertThat(ByteUnit.KIBIBYTES.toBytes(1)).isEqualTo(1024L);
    Assertions.assertThat(ByteUnit.KIBIBYTES.toBytes(2)).isEqualTo(2048L);
    Assertions.assertThat(ByteUnit.KIBIBYTES.getFactor()).isEqualTo(1024L);

    Assertions.assertThat(ByteUnit.MEBIBYTES.toBytes(1)).isEqualTo(1048576L);
    Assertions.assertThat(ByteUnit.MEBIBYTES.toBytes(2)).isEqualTo(2097152L);
    Assertions.assertThat(ByteUnit.MEBIBYTES.getFactor()).isEqualTo(1048576L);

    Assertions.assertThat(ByteUnit.GIBIBYTES.toBytes(1)).isEqualTo(1073741824L);
    Assertions.assertThat(ByteUnit.GIBIBYTES.toBytes(2)).isEqualTo(2147483648L);
    Assertions.assertThat(ByteUnit.GIBIBYTES.getFactor()).isEqualTo(1073741824L);

    Assertions.assertThat(ByteUnit.TEBIBYTES.toBytes(1)).isEqualTo(1099511627776L);
    Assertions.assertThat(ByteUnit.TEBIBYTES.toBytes(2)).isEqualTo(2199023255552L);
    Assertions.assertThat(ByteUnit.TEBIBYTES.getFactor()).isEqualTo(1099511627776L);

    Assertions.assertThat(ByteUnit.PEBIBYTES.toBytes(1)).isEqualTo(1125899906842624L);
    Assertions.assertThat(ByteUnit.PEBIBYTES.toBytes(2)).isEqualTo(2251799813685248L);
    Assertions.assertThat(ByteUnit.PEBIBYTES.getFactor()).isEqualTo(1125899906842624L);

    // SI Units
    Assertions.assertThat(ByteUnit.KILOBYTES.toBytes(1)).isEqualTo(1000L);
    Assertions.assertThat(ByteUnit.KILOBYTES.toBytes(2)).isEqualTo(2000L);
    Assertions.assertThat(ByteUnit.KILOBYTES.getFactor()).isEqualTo(1000L);

    Assertions.assertThat(ByteUnit.MEGABYTES.toBytes(1)).isEqualTo(1000000L);
    Assertions.assertThat(ByteUnit.MEGABYTES.toBytes(2)).isEqualTo(2000000L);
    Assertions.assertThat(ByteUnit.MEGABYTES.getFactor()).isEqualTo(1000000L);

    Assertions.assertThat(ByteUnit.GIGABYTES.toBytes(1)).isEqualTo(1000000000L);
    Assertions.assertThat(ByteUnit.GIGABYTES.toBytes(2)).isEqualTo(2000000000L);
    Assertions.assertThat(ByteUnit.GIGABYTES.getFactor()).isEqualTo(1000000000L);

    Assertions.assertThat(ByteUnit.TERABYTES.toBytes(1)).isEqualTo(1000000000000L);
    Assertions.assertThat(ByteUnit.TERABYTES.toBytes(2)).isEqualTo(2000000000000L);
    Assertions.assertThat(ByteUnit.TERABYTES.getFactor()).isEqualTo(1000000000000L);

    Assertions.assertThat(ByteUnit.PETABYTES.toBytes(1)).isEqualTo(1000000000000000L);
    Assertions.assertThat(ByteUnit.PETABYTES.toBytes(2)).isEqualTo(2000000000000000L);
    Assertions.assertThat(ByteUnit.PETABYTES.getFactor()).isEqualTo(1000000000000000L);
  }
}
