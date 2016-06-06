package org.lmdbjava.core.lli.exceptions;

import static java.lang.String.format;
import org.lmdbjava.core.lli.LmdbException;

/**
 * Superclass for all exceptions that originate from a native C call.
 */
public abstract class LmdbNativeException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Result code returned by the LMDB C function.
   */
  private final int rc;

  /**
   * Constructs an instance with the provided detailed message.
   *
   * @param msg the detail message.
   * @param rc  the result code.
   */
  LmdbNativeException(final int rc, final String msg) {
    super(format(msg + " (%d)", rc));
    this.rc = rc;
  }

  /**
   *
   * @return the C-side result code
   */
  public final int getResultCode() {
    return rc;
  }
}
