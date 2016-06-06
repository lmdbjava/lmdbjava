package org.lmdbjava.core.lli;

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

  /**
   * Constructs an instance with the provided detailed message and cause.
   *
   * @param message the detail message
   * @param cause   the cause
   */
  public LmdbException(String message, Throwable cause) {
    super(message, cause);
  }

}
