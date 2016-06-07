package org.lmdbjava;

/**
 * Object has not been opened.
 */
public class NotOpenException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   * @param type that has not been opened
   */
  public NotOpenException(String type) {
    super(type + " has not been opened");
  }

}
