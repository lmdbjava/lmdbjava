package org.lmdbjava.core.lli.exceptions;

/**
 * Database contents grew beyond environment mapsize.
 */
public final class MapResizedException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_MAP_RESIZED = -30_785;

  MapResizedException() {
    super(MDB_MAP_RESIZED, "Database contents grew beyond environment mapsize");
  }
}
