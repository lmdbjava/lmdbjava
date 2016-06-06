package org.lmdbjava.core.lli.exceptions;

/**
 * Environment version mismatch.
 */
public final class VersionMismatchException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_VERSION_MISMATCH = -30_794;

  VersionMismatchException() {
    super(MDB_VERSION_MISMATCH, "Environment version mismatch");
  }
}
