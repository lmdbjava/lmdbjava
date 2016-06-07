package org.lmdbjava;

/**
 * Transaction has already been committed.
 */
public final class AlreadyCommittedException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   */
  public AlreadyCommittedException() {
    super("Transaction has already been opened");
  }
}
