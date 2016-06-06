package org.lmdbjava.core.lli;

import java.util.Objects;
import java.util.Set;
import jnr.ffi.byref.IntByReference;

import static java.util.Objects.requireNonNull;
import static org.lmdbjava.core.lli.Library.lib;
import static org.lmdbjava.core.lli.Utils.mask;
import org.lmdbjava.core.lli.exceptions.LmdbNativeException;
import static org.lmdbjava.core.lli.exceptions.ResultCodeMapper.checkRc;

/**
 * LMDB Database.
 */
public final class Database {

  private final String name;
  final int dbi;

  Database(Transaction tx, String name, Set<DatabaseFlags> flags) throws
      AlreadyCommittedException, LmdbNativeException {
    requireNonNull(tx);
    requireNonNull(flags);
    requireNonNull(name);
    if (name.isEmpty()) {
      throw new IllegalArgumentException("name is empty");
    }
    if (tx.isCommitted()) {
      throw new AlreadyCommittedException();
    }
    this.name = name;
    final int flagsMask = mask(flags);
    final IntByReference dbiPtr = new IntByReference();
    checkRc(lib.mdb_dbi_open(tx.ptr, name, flagsMask, dbiPtr));
    dbi = dbiPtr.intValue();
  }

  /**
   * Obtains the name of this database.
   *
   * @return the name (never null or empty)
   */
  public String getName() {
    return name;
  }

}
