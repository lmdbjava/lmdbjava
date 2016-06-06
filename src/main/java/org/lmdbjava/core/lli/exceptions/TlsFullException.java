package org.lmdbjava.core.lli.exceptions;

/**
 * Too many TLS keys in use - Windows only.
 */
public final class TlsFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_TLS_FULL = -30_789;

  TlsFullException() {
    super(MDB_TLS_FULL, "Too many TLS keys in use - Windows only");
  }
}
