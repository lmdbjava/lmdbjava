package org.lmdbjava;

/**
 * Database contents grew beyond environment mapsize.
 */
public final class DatabaseMapResizedException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_MAP_RESIZED = -30_785;

  DatabaseMapResizedException() {
    super(MDB_MAP_RESIZED, "Database contents grew beyond environment mapsize");
  }
}
