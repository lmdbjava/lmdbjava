package org.lmdbjava;

/**
 * Flags for use when performing a "put".
 */
public enum PutFlags implements MaskedFlag {

  /**
   * For put: Don't write if the key already exists.
   */
  MDB_NOOVERWRITE(0x10),
  /**
   * Only for #MDB_DUPSORT<br>
   * For put: don't write if the key and data pair already exist.<br>
   * For mdb_cursor_del: remove all duplicate data items.
   */
  MDB_NODUPDATA(0x20),
  /**
   * For mdb_cursor_put: overwrite the current key/data pair
   */
  MDB_CURRENT(0x40),
  /**
   * For put: Just reserve space for data, don't copy it. Return a pointer to
   * the reserved space.
   */
  MDB_RESERVE(0x1_0000),
  /**
   * Data is being appended, don't split full pages.
   */
  MDB_APPEND(0x2_0000),
  /**
   * Duplicate data is being appended, don't split full pages.
   */
  MDB_APPENDDUP(0x4_0000),
  /**
   * Store multiple data items in one call. Only for #MDB_DUPFIXED.
   */
  MDB_MULTIPLE(0x8_0000);

  private final int mask;

  PutFlags(final int mask) {
    this.mask = mask;
  }

  @Override
  public int getMask() {
    return mask;
  }

}
