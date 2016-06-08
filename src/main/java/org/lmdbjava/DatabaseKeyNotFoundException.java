package org.lmdbjava;

/**
 * Key/data pair not found (EOF).
 */
public final class DatabaseKeyNotFoundException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_NOTFOUND = -30_798;

  DatabaseKeyNotFoundException() {
    super(MDB_NOTFOUND, "key/data pair not found (EOF)");
  }
}
