package org.lmdbjava;

/**
 * Exception raised from a system constant table lookup.
 */
public final class ConstantDerviedException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;

  ConstantDerviedException(final int rc, final String message) {
    super(rc, "Platform constant error code: " + message);
  }
}
