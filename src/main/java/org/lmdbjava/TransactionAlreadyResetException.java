package org.lmdbjava;

/**
 * The current transaction has already been reset.
 */
public class TransactionAlreadyResetException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   */
  public TransactionAlreadyResetException() {
    super("Transaction has already been reset");
  }

}
