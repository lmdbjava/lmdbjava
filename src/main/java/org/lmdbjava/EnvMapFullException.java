package org.lmdbjava;

/**
 * Environment mapsize reached.
 */
public final class EnvMapFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_MAP_FULL = -30_792;

  EnvMapFullException() {
    super(MDB_MAP_FULL, "Environment mapsize reached");
  }
}
