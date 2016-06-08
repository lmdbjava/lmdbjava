package org.lmdbjava;

/**
 * File is not a valid LMDB file.
 */
public final class FileInvalidException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_INVALID = -30_793;

  FileInvalidException() {
    super(MDB_INVALID, "File is not a valid LMDB file");
  }
}
