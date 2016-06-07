package org.lmdbjava;

/**
 * Key/data pair already exists.
 */
public final class KeyExistsException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_KEYEXIST = -30_799;

  KeyExistsException() {
    super(MDB_KEYEXIST, "key/data pair already exists");
  }
}
