package org.lmdbjava;

/**
 * Environment version mismatch.
 */
public final class EnvVersionMismatchException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_VERSION_MISMATCH = -30_794;

  EnvVersionMismatchException() {
    super(MDB_VERSION_MISMATCH, "Environment version mismatch");
  }
}
