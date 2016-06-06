package org.lmdbjava.core.lli;

import java.util.Set;
import static org.lmdbjava.core.support.Validate.notNull;

/**
 * Utility methods used internally by this package.
 */
final class Utils {

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param flags to mask
   * @return the integer mask for use in C
   */
  static int mask(Set<? extends MaskedFlag> flags) {
    notNull(flags);
    int result = 0;
    for (MaskedFlag flag : flags) {
      result |= flag.getMask();
    }
    return result;
  }

  private Utils() {
  }

}
