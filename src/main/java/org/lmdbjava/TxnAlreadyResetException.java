package org.lmdbjava;

/**
 * The current transaction has already been reset.
 */
public class TxnAlreadyResetException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   */
  public TxnAlreadyResetException() {
    super("Transaction has already been reset");
  }

}
