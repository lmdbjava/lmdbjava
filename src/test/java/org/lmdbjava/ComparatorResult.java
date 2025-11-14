package org.lmdbjava;

/** Converts an integer result code into its contractual meaning. */
enum ComparatorResult {
  LESS_THAN,
  EQUAL_TO,
  GREATER_THAN;

  static ComparatorResult get(final int comparatorResult) {
    if (comparatorResult == 0) {
      return EQUAL_TO;
    }
    return comparatorResult < 0 ? LESS_THAN : GREATER_THAN;
  }

  ComparatorResult opposite() {
    if (this == LESS_THAN) {
      return GREATER_THAN;
    } else if (this == GREATER_THAN) {
      return LESS_THAN;
    } else {
      return EQUAL_TO;
    }
  }
}
