package org.lmdbjava.core.lli.exceptions;

/**
 * Environment maxdbs reached.
 */
public final class DatabasesFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_DBS_FULL = -30_791;

  DatabasesFullException() {
    super(MDB_DBS_FULL, "Environment maxdbs reached");
  }
}
