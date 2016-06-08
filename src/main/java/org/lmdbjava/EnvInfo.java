package org.lmdbjava;

/**
 * Environment information.
 */
public class EnvInfo {

  public final long lastPageNumber;
  public final long lastTransactionId;
  public final long mapAddress;
  public final long mapSize;
  public final int maxReaders;
  public final int numReaders;

  public EnvInfo(long mapAddress, long mapSize, long lastPageNumber,
                 long lastTransactionId, int maxReaders, int numReaders) {
    this.mapAddress = mapAddress;
    this.mapSize = mapSize;
    this.lastPageNumber = lastPageNumber;
    this.lastTransactionId = lastTransactionId;
    this.maxReaders = maxReaders;
    this.numReaders = numReaders;
  }

}
