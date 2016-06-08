package org.lmdbjava;

/**
 * Transaction has already been committed.
 */
public final class TxnAlreadyCommittedException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   */
  public TxnAlreadyCommittedException() {
    super("Transaction has already been opened");
  }
}
