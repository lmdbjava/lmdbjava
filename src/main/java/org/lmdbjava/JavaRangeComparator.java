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


import java.util.Comparator;
import java.util.function.Supplier;

class JavaRangeComparator<T> implements RangeComparator {

  private final Comparator<T> comparator;
  private final Supplier<T> currentKeySupplier;
  private final T start;
  private final T stop;

  JavaRangeComparator(
      final KeyRange<T> range,
      final Comparator<T> comparator,
      final Supplier<T> currentKeySupplier) {
    this.comparator = comparator;
    this.currentKeySupplier = currentKeySupplier;
    this.start = range.getStart();
    this.stop = range.getStop();
  }

  @Override
  public int compareToStartKey() {
    return comparator.compare(currentKeySupplier.get(), start);
  }

  @Override
  public int compareToStopKey() {
    return comparator.compare(currentKeySupplier.get(), stop);
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }
}
