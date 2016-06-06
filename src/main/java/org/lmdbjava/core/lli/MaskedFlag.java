package org.lmdbjava.core.lli;

/**
 * Indicates an enum that can provide integers for each of its values,
 * <p>
 * These values can be masked together via {@link Utils#mask(java.util.Set)}.
 */
public interface MaskedFlag {

  /**
   * Obtains the integer value for this enum which can be included in a mask.
   *
   * @return the integer value for combination into a mask
   */
  int getMask();

}
