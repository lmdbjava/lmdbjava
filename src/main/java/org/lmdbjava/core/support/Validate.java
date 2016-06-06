package org.lmdbjava.core.support;

/**
 * Validation utility methods.
 */
public final class Validate {

  /**
   * Ensures the passed string is not null or empty.
   *
   * @param str to verify
   */
  public static void hasLength(String str) {
    if (str == null || str.isEmpty()) {
      throw new IllegalArgumentException("required");
    }
  }

  /**
   * Ensures the object is not null.
   *
   * @param o   to verify
   * @param msg to include in the exception
   */
  public static void notNull(final Object o, final String msg) {
    if (o == null) {
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Ensures the object is not null.
   *
   * @param o to verify
   */
  public static void notNull(final Object o) {
    if (o == null) {
      throw new IllegalArgumentException("required");
    }
  }

  private Validate() {
  }

}
