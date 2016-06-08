package org.lmdbjava;

/**
 * The current transaction is not a read-only transaction.
 */
public class ReadOnlyTransactionRequiredException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   */
  public ReadOnlyTransactionRequiredException() {
    super("Not a read-only transaction");
  }

}
