package org.lmdbjava;

import static java.util.Objects.requireNonNull;
import java.util.Set;

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

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param flags to mask
   * @return the integer mask for use in C
   */
  static int mask(Set<? extends MaskedFlag> flags) {
    requireNonNull(flags);
    int result = 0;
    for (MaskedFlag flag : flags) {
      result |= flag.getMask();
    }
    return result;
  }
}
