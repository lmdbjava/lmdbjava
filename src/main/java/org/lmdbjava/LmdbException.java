package org.lmdbjava;

/**
 * Superclass for all LmdbJava custom exceptions.
 */
public class LmdbException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs an instance with the provided detailed message.
   *
   * @param message the detail message
   */
  public LmdbException(final String message) {
    super(message);
  }
}
