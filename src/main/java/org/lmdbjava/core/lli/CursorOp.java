package org.lmdbjava.core.lli;

/**
 * Flags for use when performing a cursor operation.
 * <p>
 * Unlike all other LMDB enums in the LLI package, this enum is not bit masked.
 */
public enum CursorOp {

  /**
   * Position at first key/data item
   */
  MDB_FIRST(0),
  /**
   * Position at first data item of current key. Only for
   * {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_FIRST_DUP(1),
  /**
   * Position at key/data pair. Only for {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_GET_BOTH(2),
  /**
   * position at key, nearest data. Only for {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_GET_BOTH_RANGE(3),
  /**
   * Return key/data at current cursor position
   */
  MDB_GET_CURRENT(4),
  /**
   * Return key and up to a page of duplicate data items from current cursor
   * position. Move cursor to prepare for {@link #MDB_NEXT_MULTIPLE}. Only for
   * {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_GET_MULTIPLE(5),
  /**
   * Position at last key/data item
   */
  MDB_LAST(6),
  /**
   * Position at last data item of current key. Only for
   * {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_LAST_DUP(7),
  /**
   * Position at next data item
   */
  MDB_NEXT(8),
  /**
   * Position at next data item of current key. Only for
   * {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_NEXT_DUP(9),
  /**
   * Return key and up to a page of duplicate data items from next cursor
   * position. Move cursor to prepare for {@link #MDB_NEXT_MULTIPLE}. Only for
   * {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_NEXT_MULTIPLE(10),
  /**
   * Position at first data item of next key
   */
  MDB_NEXT_NODUP(11),
  /**
   * Position at previous data item
   */
  MDB_PREV(12),
  /**
   * Position at previous data item of current key.
   * {@link DatabaseFlags#MDB_DUPSORT}.
   */
  MDB_PREV_DUP(13),
  /**
   * Position at last data item of previous key
   */
  MDB_PREV_NODUP(14),
  /**
   * Position at specified key
   */
  MDB_SET(15),
  /**
   * Position at specified key, return key + data
   */
  MDB_SET_KEY(16),
  /**
   * Position at first key greater than or equal to specified key
   */
  MDB_SET_RANGE(17);

  private final int code;

  /**
   * Obtain the integer code for use by LMDB C API.
   *
   * @return the code
   */
  public int getCode() {
    return code;
  }

  CursorOp(final int code) {
    this.code = code;
  }

}
