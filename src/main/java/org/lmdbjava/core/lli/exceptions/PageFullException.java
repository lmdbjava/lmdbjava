package org.lmdbjava.core.lli.exceptions;

/**
 * Page has not enough space - internal error.
 */
public final class PageFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_PAGE_FULL = -30_786;

  PageFullException() {
    super(MDB_PAGE_FULL, "Page has not enough space - internal error");
  }
}
