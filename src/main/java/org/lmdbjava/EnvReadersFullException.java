package org.lmdbjava;

/**
 * Environment maxreaders reached.
 */
public final class EnvReadersFullException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_READERS_FULL = -30_790;

  EnvReadersFullException() {
    super(MDB_READERS_FULL, "Environment maxreaders reached");
  }
}
