package org.lmdbjava.core.lli.exceptions;

/**
 * Environment maxreaders reached.
 */
public final class ReadersFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_READERS_FULL = -30_790;

  ReadersFullException() {
    super(MDB_READERS_FULL, "Environment maxreaders reached");
  }
}
