package org.lmdbjava;

/**
 * Flags for use when creating a {@link Txn}.
 */
public enum TxnFlags implements MaskedFlag {
  /**
   * Read only
   */
  MDB_RDONLY(0x2_0000);

  private final int mask;

  TxnFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
