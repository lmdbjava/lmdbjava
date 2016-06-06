package org.lmdbjava.core.lli;

import static org.lmdbjava.core.lli.Library.lib;

/**
 * Superclass for all exceptions that originate from a native C call.
 */
public class LmdbNativeException extends LmdbException {

  private static final long serialVersionUID = 1L;

  /**
   * Return code returned by the LMDB C function.
   */
  private final int rc;

  /**
   * Constructs an instance with the provided detailed message.
   *
   * @param rc  the result code.
   */
  LmdbNativeException(final int rc) {
    super();
    this.rc = rc;
  }

  /**
   * Constructs an instance with the provided detailed message.
   *
   * @param rc  the result code.
   * @param msg the detail message.
   */
  LmdbNativeException(final int rc, String msg) {
    super(msg);
    this.rc = rc;
  }

  @Override
  public String getMessage() {
    if (super.getMessage() == null) {
      return lib.mdb_strerror(rc).getString(0);
    }
    return super.getMessage();
  }

  /**
   *
   * @return the C-side return code
   */
  public final int getReturnCode() {
    return rc;
  }
}
