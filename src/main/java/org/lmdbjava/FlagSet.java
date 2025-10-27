package org.lmdbjava;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A set of flags, each with a bit mask value.
 * Flags can be combined in a set such that the set has a combined bit mask value.
 * @param <T>
 */
public interface FlagSet<T extends MaskedFlag> extends Iterable<T> {

  int getMask();

  Set<T> getFlags();

  boolean isSet(T flag);

  default int size() {
    return getFlags().size();
  }

  default boolean isEmpty() {
    return getFlags().isEmpty();
  }

  default Iterator<T> iterator() {
    return getFlags().iterator();
  }

  static <T extends MaskedFlag> String asString(final FlagSet<T> flagSet) {
    Objects.requireNonNull(flagSet);
    final String flagsStr = flagSet.getFlags()
        .stream()
        .sorted(Comparator.comparing(MaskedFlag::getMask))
        .map(MaskedFlag::name)
        .collect(Collectors.joining(", "));
    return "FlagSet{" +
        "flags=[" + flagsStr +
        "], mask=" + flagSet.getMask() +
        '}';
  }

  static boolean equals(final FlagSet<?> flagSet1,
                        final FlagSet<?> flagSet2) {
    if (flagSet1 == flagSet2) {
      return true;
    } else if (flagSet1 != null && flagSet2 == null) {
      return false;
    } else if (flagSet1 == null) {
      return false;
    } else {
      return flagSet1.getMask() == flagSet2.getMask()
          && Objects.equals(flagSet1.getFlags(), flagSet2.getFlags());
    }
  }

}
