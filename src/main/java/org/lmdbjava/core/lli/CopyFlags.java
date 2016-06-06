package org.lmdbjava.core.lli;

/**
 * Flags for use when performing a "put".
 */
public enum CopyFlags implements MaskedFlag {

  /**
   * Compacting copy: Omit free space from copy, and renumber all pages
   * sequentially.
   */
  MDB_CP_COMPACT(0x01);

  private final int mask;

  CopyFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
