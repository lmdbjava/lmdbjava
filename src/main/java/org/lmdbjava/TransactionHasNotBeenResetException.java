package org.lmdbjava;

/**
 * The current transaction has not been reset.
 */
public class TransactionHasNotBeenResetException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   */
  public TransactionHasNotBeenResetException() {
    super("Transaction has not been reset");
  }

}
