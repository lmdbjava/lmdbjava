package org.lmdbjava;

/** For comparing a cursor's current key against a {@link KeyRange}'s start/stop key. */
interface RangeComparator extends AutoCloseable {

  /**
   * Compare the cursor's current key to the range start key. Equivalent to compareTo(currentKey,
   * startKey)
   */
  int compareToStartKey();

  /**
   * Compare the cursor's current key to the range stop key. Equivalent to compareTo(currentKey,
   * stopKey)
   */
  int compareToStopKey();
}
