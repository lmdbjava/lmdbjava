package org.lmdbjava;

/**
 * Transaction has too many dirty pages.
 */
public final class TransactionFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_TXN_FULL = -30_788;

  TransactionFullException() {
    super(MDB_TXN_FULL, "Transaction has too many dirty pages");
  }
}
