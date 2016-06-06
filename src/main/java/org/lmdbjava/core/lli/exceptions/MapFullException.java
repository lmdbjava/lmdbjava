package org.lmdbjava.core.lli.exceptions;

/**
 * Environment mapsize reached.
 */
public final class MapFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_MAP_FULL = -30_792;

  MapFullException() {
    super(MDB_MAP_FULL, "Environment mapsize reached");
  }
}
