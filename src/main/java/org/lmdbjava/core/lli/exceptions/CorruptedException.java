package org.lmdbjava.core.lli.exceptions;

/**
 * Located page was wrong type.
 */
public final class CorruptedException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_CORRUPTED = -30_796;

  CorruptedException() {
    super(MDB_CORRUPTED, "located page was wrong type");
  }
}
