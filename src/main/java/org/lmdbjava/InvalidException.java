package org.lmdbjava;

/**
 * File is not a valid LMDB file.
 */
public final class InvalidException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_INVALID = -30_793;

  InvalidException() {
    super(MDB_INVALID, "File is not a valid LMDB file");
  }
}
