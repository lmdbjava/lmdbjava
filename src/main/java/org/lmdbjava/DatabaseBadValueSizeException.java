package org.lmdbjava;

/**
 * Unsupported size of key/DB name/data, or wrong DUPFIXED size.
 */
public final class DatabaseBadValueSizeException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_BAD_VALSIZE = -30_781;

  DatabaseBadValueSizeException() {
    super(MDB_BAD_VALSIZE,
          "Unsupported size of key/DB name/data, or wrong DUPFIXED size");
  }
}
