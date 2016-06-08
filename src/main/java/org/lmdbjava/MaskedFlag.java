package org.lmdbjava;

import static java.util.Objects.nonNull;

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
   * @param flags to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
  static int mask(final MaskedFlag... flags) {
    if (flags == null || flags.length == 0) {
      return 0;
    }

    int result = 0;
    for (MaskedFlag flag : flags) {
      result |= flag.getMask();
    }
    return result;
  }

  /**
   * Indicates whether the passed flag has the relevant masked flag high.
   *
   * @param flags to evaluate (usually produced by
   *              {@link #mask(org.lmdbjava.MaskedFlag...)}
   * @param test  the flag being sought (required)
   * @return
   */
  static boolean isSet(final int flags, final MaskedFlag test) {
    nonNull(test);
    return (flags & test.getMask()) == test.getMask();
  }
}
