package org.lmdbjava.core.lli.exceptions;

/**
 * Operation and DB incompatible, or DB type changed.
 * <p>
 * This can mean:
 * <ul>
 * <li>The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.</li>
 * <li>Opening a named DB when the unnamed DB has MDB_DUPSORT /
 * MDB_INTEGERKEY.</li>
 * <li>Accessing a data record as a database, or vice versa.</li>
 * <li>The database was dropped and recreated with different flags.</li>
 * </ul>
 */
public final class IncompatibleException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_INCOMPATIBLE = -30_784;

  IncompatibleException() {
    super(MDB_INCOMPATIBLE, "Operation and DB incompatible, or DB type changed");
  }
}
