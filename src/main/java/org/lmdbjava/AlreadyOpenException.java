package org.lmdbjava;

/**
 * Object has already been opened and the operation is therefore prohibited.
 */
public final class AlreadyOpenException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   * @param type that has already been opened
   */
  public AlreadyOpenException(String type) {
    super(type + " has already been opened");
  }

}
