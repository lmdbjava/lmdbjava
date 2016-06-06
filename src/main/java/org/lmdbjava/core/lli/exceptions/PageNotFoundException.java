package org.lmdbjava.core.lli.exceptions;

/**
 * Requested page not found - this usually indicates corruption.
 */
public final class PageNotFoundException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_PAGE_NOTFOUND = -30_797;

  PageNotFoundException() {
    super(MDB_PAGE_NOTFOUND,
          "Requested page not found - this usually indicates corruption");
  }
}
