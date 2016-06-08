package org.lmdbjava;

/**
 * Located page was wrong type.
 */
public final class PageCorruptedException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_CORRUPTED = -30_796;

  PageCorruptedException() {
    super(MDB_CORRUPTED, "located page was wrong type");
  }
}
