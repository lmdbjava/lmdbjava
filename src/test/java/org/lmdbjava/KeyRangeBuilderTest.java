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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Tests the {@link KeyRange} builder, prefix and start/stop-inclusive accessors added for gh-269.
 *
 * <p>{@link KeyRangeTest} already covers the iteration contract of the static {@link KeyRangeType}
 * factories via a fake cursor; this test focuses on the newer construction paths and their exposed
 * flags, which were largely uncovered.
 */
final class KeyRangeBuilderTest {

  @Test
  void builderForwardStartInclusiveStopExclusive() {
    final KeyRange<Integer> range = KeyRange.builder().startInclusive(2).stopExclusive(8).build();
    assertThat(range.getStart()).isEqualTo(2);
    assertThat(range.getStop()).isEqualTo(8);
    assertThat(range.isStartKeyInclusive()).isTrue();
    assertThat(range.isStopKeyInclusive()).isFalse();
    assertThat(range.isDirectionForward()).isTrue();
    assertThat(range.getPrefix()).isNull();
  }

  @Test
  void builderReversedStartExclusiveStopInclusive() {
    final KeyRange<Integer> range =
        KeyRange.<Integer>builder().startExclusive(2).reverse().stopInclusive(8).build();
    assertThat(range.getStart()).isEqualTo(2);
    assertThat(range.getStop()).isEqualTo(8);
    assertThat(range.isStartKeyInclusive()).isFalse();
    assertThat(range.isStopKeyInclusive()).isTrue();
    assertThat(range.isDirectionForward()).isFalse();
  }

  @Test
  void builderEmptyIsForwardAll() {
    final KeyRange<Integer> range = KeyRange.builder().build();
    assertThat(range.getStart()).isNull();
    assertThat(range.getStop()).isNull();
    assertThat(range.getPrefix()).isNull();
    assertThat(range.isDirectionForward()).isTrue();
  }

  @Test
  void builderReverseFlagToggles() {
    assertThat(KeyRange.<Integer>builder().prefix(5).reverse(true).build().isDirectionForward())
        .isFalse();
    assertThat(KeyRange.<Integer>builder().prefix(5).reverse(false).build().isDirectionForward())
        .isTrue();
  }

  @Test
  void prefixBuilder() {
    final KeyRange<Integer> range = KeyRange.builder().prefix(5).build();
    assertThat(range.getPrefix()).isEqualTo(5);
    assertThat(range.getStart()).isNull();
    assertThat(range.getStop()).isNull();
    assertThat(range.isDirectionForward()).isTrue();

    final KeyRange<Integer> reversed = KeyRange.<Integer>builder().prefix(5).reverse().build();
    assertThat(reversed.getPrefix()).isEqualTo(5);
    assertThat(reversed.isDirectionForward()).isFalse();
  }

  @Test
  void staticPrefixFactories() {
    assertThat(KeyRange.prefix(5).getPrefix()).isEqualTo(5);
    assertThat(KeyRange.prefix(5).isDirectionForward()).isTrue();
    assertThat(KeyRange.prefixBackward(5).getPrefix()).isEqualTo(5);
    assertThat(KeyRange.prefixBackward(5).isDirectionForward()).isFalse();
  }

  @Test
  void nullPrefixRejected() {
    assertThatThrownBy(() -> KeyRange.prefix(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> KeyRange.builder().prefix(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void staticFactoryInclusivityFlags() {
    // closed  = [start, stop]
    assertThat(KeyRange.closed(2, 8).isStartKeyInclusive()).isTrue();
    assertThat(KeyRange.closed(2, 8).isStopKeyInclusive()).isTrue();
    // open    = (start, stop)
    assertThat(KeyRange.open(2, 8).isStartKeyInclusive()).isFalse();
    assertThat(KeyRange.open(2, 8).isStopKeyInclusive()).isFalse();
    // closedOpen = [start, stop)
    assertThat(KeyRange.closedOpen(2, 8).isStartKeyInclusive()).isTrue();
    assertThat(KeyRange.closedOpen(2, 8).isStopKeyInclusive()).isFalse();
    // openClosed = (start, stop]
    assertThat(KeyRange.openClosed(2, 8).isStartKeyInclusive()).isFalse();
    assertThat(KeyRange.openClosed(2, 8).isStopKeyInclusive()).isTrue();
    // single-bound ranges
    assertThat(KeyRange.atLeast(2).isStartKeyInclusive()).isTrue();
    assertThat(KeyRange.greaterThan(2).isStartKeyInclusive()).isFalse();
    assertThat(KeyRange.atMost(8).isStopKeyInclusive()).isTrue();
    assertThat(KeyRange.lessThan(8).isStopKeyInclusive()).isFalse();
  }

  @Test
  void staticFactoriesExposeType() {
    assertThat(KeyRange.all().getType()).isEqualTo(KeyRangeType.FORWARD_ALL);
    assertThat(KeyRange.allBackward().getType()).isEqualTo(KeyRangeType.BACKWARD_ALL);
    assertThat(KeyRange.closed(2, 8).getType()).isEqualTo(KeyRangeType.FORWARD_CLOSED);
    assertThat(KeyRange.all().isDirectionForward()).isTrue();
    assertThat(KeyRange.allBackward().isDirectionForward()).isFalse();
  }

  @Test
  void builderAndPrefixRangesHaveNullType() {
    // NOTE: documents current (surprising) behaviour. The private constructors used by builder()
    // and prefix() never set `type`, so getType() is null for these ranges. The new
    // LmdbIterable/LmdbStream paths only read directionForward/getPrefix/getStart/getStop, so this
    // is currently harmless, but passing such a range to the legacy CursorIterable (Dbi.iterate),
    // which calls getType(), would NPE. This assertion will flag the day that changes.
    assertThat(KeyRange.builder().startInclusive(2).stopExclusive(8).build().getType()).isNull();
    assertThat(KeyRange.builder().build().getType()).isNull();
    assertThat(KeyRange.prefix(5).getType()).isNull();
    assertThat(KeyRange.prefixBackward(5).getType()).isNull();
  }
}
