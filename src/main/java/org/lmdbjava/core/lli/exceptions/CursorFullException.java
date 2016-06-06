package org.lmdbjava.core.lli.exceptions;

/**
 * Cursor stack too deep - internal error.
 */
public final class CursorFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_CURSOR_FULL = -30_787;

  CursorFullException() {
    super(MDB_CURSOR_FULL, "Cursor stack too deep - internal error");
  }
}
