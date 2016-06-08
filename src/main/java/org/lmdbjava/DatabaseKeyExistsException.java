package org.lmdbjava;

/**
 * Key/data pair already exists.
 */
public final class DatabaseKeyExistsException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_KEYEXIST = -30_799;

  DatabaseKeyExistsException() {
    super(MDB_KEYEXIST, "key/data pair already exists");
  }
}
