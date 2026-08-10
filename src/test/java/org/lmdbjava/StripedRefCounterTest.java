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

import org.junit.jupiter.api.Test;

class StripedRefCounterTest {

  @Test
  void lowestPowerOfTwoGreaterThanOrEqualTo() {
    // Test powers of two
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(1)).isEqualTo(1);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(2)).isEqualTo(2);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(4)).isEqualTo(4);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(8)).isEqualTo(8);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(16)).isEqualTo(16);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(1024)).isEqualTo(1024);

    // Test non-powers of two
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(3)).isEqualTo(4);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(5)).isEqualTo(8);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(7)).isEqualTo(8);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(15)).isEqualTo(16);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(24)).isEqualTo(32);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(100)).isEqualTo(128);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(1000)).isEqualTo(1024);

    // Test edge cases
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(536870912))
        .isEqualTo(536870912);
    assertThat(StripedRefCounter.lowestPowerOfTwoGreaterThanOrEqualTo(536870913))
        .isEqualTo(1073741824);
  }

  @Test
  void getStripeCount() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter();
    assertThat(stripedRefCounter.getStripeCount()).isGreaterThan(1);
  }

  @Test
  void getStripeCount2() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter(16);
    assertThat(stripedRefCounter.getStripeCount()).isEqualTo(16);
  }

  @Test
  void getStripeCount3() {
    final StripedRefCounter stripedRefCounter = new StripedRefCounter(15);
    assertThat(stripedRefCounter.getStripeCount()).isEqualTo(16);
  }

  @Test
  void getStripeCount4() {
    assertThatThrownBy(() -> new StripedRefCounter(99999999))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getStripeCount5() {
    assertThatThrownBy(() -> new StripedRefCounter(0)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getStripeCount6() {
    assertThatThrownBy(() -> new StripedRefCounter(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
