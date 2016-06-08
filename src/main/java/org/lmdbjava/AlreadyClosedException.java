package org.lmdbjava;

/**
 * Object has already been closed and the operation is therefore prohibited.
 */
public final class AlreadyClosedException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   * @param type that has already been closed
   */
  public AlreadyClosedException(final String type) {
    super(type + " has already been closed");
  }

}
