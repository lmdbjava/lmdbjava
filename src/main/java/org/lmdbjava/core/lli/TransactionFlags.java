package org.lmdbjava.core.lli;

/**
 * Flags for use when creating a {@link Transaction}.
 */
public enum TransactionFlags implements MaskedFlag {
  /**
   * Read only
   */
  MDB_RDONLY(0x2_0000);

  private final int mask;

  TransactionFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
