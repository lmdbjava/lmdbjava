package org.lmdbjava.core.lli.exceptions;

/**
 * Invalid reuse of reader locktable slot.
 */
public final class BadReaderLockTableSlotException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_BAD_RSLOT = -30_783;

  BadReaderLockTableSlotException() {
    super(MDB_BAD_RSLOT, "Invalid reuse of reader locktable slot");
  }
}
