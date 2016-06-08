package org.lmdbjava;

/**
 * The current transaction has not been reset.
 */
public class TxnHasNotBeenResetException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   */
  public TxnHasNotBeenResetException() {
    super("Transaction has not been reset");
  }

}
