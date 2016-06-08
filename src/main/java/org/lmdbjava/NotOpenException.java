package org.lmdbjava;

/**
 * Object has is not open (eg never opened, or since closed).
 */
public class NotOpenException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance.
   *
   * @param type that has not been opened
   */
  public NotOpenException(String type) {
    super(type + " is not open");
  }

}
