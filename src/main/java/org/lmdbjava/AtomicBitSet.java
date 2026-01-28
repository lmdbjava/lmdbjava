package org.lmdbjava;


import java.util.concurrent.atomic.AtomicLong;

public class AtomicBitSet {
  private static final int MAX_SIZE = (Long.BYTES * 8);

  private final int size;
  private final int maxIdx;
  private final AtomicLong atomicLong = new AtomicLong(0);

  public AtomicBitSet() {
    this(MAX_SIZE);
  }

  public AtomicBitSet(int size) {
    if (size < 0 || size > MAX_SIZE) {
      throw new IllegalArgumentException("size must be between 0 and " + MAX_SIZE + " (inclusive)");
    }
    this.maxIdx = size - 1;
    this.size = size;
  }

  public boolean flip(int idx) {
    checkIdx(idx);
    final long newVal = atomicLong.accumulateAndGet(idx, (currVal, idx2) ->
        currVal ^ (1L << idx2));
    return isSetWithNoCheck(newVal, idx);
  }

  /**
   * Set the bit at position idx and return the resulting bit set as a long.
   */
  public long setAndGet(int idx) {
    checkIdx(idx);
    return atomicLong.accumulateAndGet(idx, (currVal, idx2) ->
        currVal | (1L << idx2));
  }

  /**
   * Sets the bit at position idx
   *
   * @return The previous value of the set as a long.
   */
  public long getAndSet(int idx) {
    checkIdx(idx);
    return atomicLong.getAndAccumulate(idx, (currVal, idx2) ->
        currVal | (1L << idx2));
  }

  /**
   * Un-set the bit at position idx and return the resulting bit set as a long.
   */
  public long unset(int idx) {
    checkIdx(idx);
    return atomicLong.updateAndGet(currVal ->
        currVal & ~(1L << idx));
  }

  /**
   * Set/un-set the bit at position idx, according to the value of isSet,
   * and return the resulting bit set as a long.
   */
  public long setAndGet(int idx, final boolean isSet) {
    return isSet
        ? setAndGet(idx)
        : unset(idx);
  }

  /**
   * @return True if the bit at position idx is set.
   */
  public boolean isSet(final int idx) {
    checkIdx(idx);
    return isSetWithNoCheck(atomicLong.get(), idx);
  }

  /**
   * @return The number of bits that have been set.
   */
  public int countSet() {
    return Long.bitCount(atomicLong.get());
  }

  public int countSet(final long val) {
    return Long.bitCount(val);
  }

  /**
   * @return The number of bits that are un-set.
   */
  public int countUnSet() {
    return size - Long.bitCount(atomicLong.get());
  }

  public int countUnSet(final long val) {
    return size - Long.bitCount(val);
  }

  public void unSetAll() {
    atomicLong.set(0L);
  }

  public void setAll() {
    if (size == MAX_SIZE) {
      atomicLong.set(-1L);
    } else {
      for (int i = 0; i < size; i++) {
        setAndGet(i);
      }
    }
  }

  public long asLong() {
    return atomicLong.get();
  }

  public boolean isSet(final long val, final int idx) {
    checkIdx(idx);
    return isSetWithNoCheck(val, idx);
  }

  private static boolean isSetWithNoCheck(final long val, final int idx) {
    return ((val >> idx) & 1L) != 0L;
  }

  private void checkIdx(final int idx) {
    if (idx < 0 || idx > maxIdx) {
      throw new IllegalArgumentException("idx must be between 0 and " + maxIdx + " (inclusive)");
    }
  }

  @Override
  public String toString() {
    return Long.toBinaryString(atomicLong.get());
  }
}
