package org.lmdbjava;

/**
 * Transaction must abort, has a child, or is invalid.
 */
public final class TxnBadException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_BAD_TXN = -30_782;

  TxnBadException() {
    super(MDB_BAD_TXN, "Transaction must abort, has a child, or is invalid");
  }
}
