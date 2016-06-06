package org.lmdbjava.core.lli.exceptions;

/**
 * Key/data pair not found (EOF).
 */
public final class NotFoundException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_NOTFOUND = -30_798;

  NotFoundException() {
    super(MDB_NOTFOUND, "key/data pair not found (EOF)");
  }
}
