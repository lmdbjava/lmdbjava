package org.lmdbjava.core.lli.exceptions;

/**
 * Update of meta page failed or environment had fatal error.
 */
public final class PanicException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_PANIC = -30_795;

  PanicException() {
    super(MDB_PANIC, "Update of meta page failed or environment had fatal error");
  }
}
