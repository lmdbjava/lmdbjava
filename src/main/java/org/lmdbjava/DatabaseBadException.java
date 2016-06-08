package org.lmdbjava;

/**
 * The specified DBI was changed unexpectedly.
 */
public final class DatabaseBadException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_BAD_DBI = -30_780;

  DatabaseBadException() {
    super(MDB_BAD_DBI, "The specified DBI was changed unexpectedly");
  }
}
