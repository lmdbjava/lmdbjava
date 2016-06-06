package org.lmdbjava.core.lli.exceptions;

/**
 * The specified DBI was changed unexpectedly.
 */
public final class BadDatabaseIdException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_BAD_DBI = -30_780;

  BadDatabaseIdException() {
    super(MDB_BAD_DBI, "The specified DBI was changed unexpectedly");
  }
}
