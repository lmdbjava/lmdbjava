package org.lmdbjava;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Encapsulates an immutable set of flags and the associated bit mask for the flags in the set.
 *
 * @param <T>
 */
public abstract class AbstractFlagSet<T extends Enum<T> & MaskedFlag> implements Iterable<T> {

  private final Set<T> flags;
  private final int mask;

  protected AbstractFlagSet(final EnumSet<T> flags) {
    Objects.requireNonNull(flags);
    this.mask = MaskedFlag.mask(flags);
    this.flags = Collections.unmodifiableSet(Objects.requireNonNull(flags));
  }

  /**
   * @return THe combined bit mask for all flags in the set.
   */
  int getMask() {
    return mask;
  }

  /**
   * @return All flags in the set.
   */
  public Set<T> getFlags() {
    return flags;
  }

  /**
   * @return True if flag has been set, i.e. is contained in this set.
   */
  public boolean isSet(final T flag) {
    return flag != null
        && flags.contains(flag);
  }

  /**
   * @return The number of flags in this set.
   */
  public int size() {
    return flags.size();
  }

  /**
   * @return True if this set is empty.
   */
  public boolean isEmpty() {
    return flags.isEmpty();
  }

  /**
   * @return The {@link Iterator} for this set.
   */
  @Override
  public Iterator<T> iterator() {
    return flags.iterator();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || getClass() != object.getClass()) return false;
    AbstractFlagSet<?> flagSet = (AbstractFlagSet<?>) object;
    return mask == flagSet.mask && Objects.equals(flags, flagSet.flags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(flags, mask);
  }

  @Override
  public String toString() {
    final String flagsStr = flags.stream()
        .sorted(Comparator.comparing(MaskedFlag::getMask))
        .map(MaskedFlag::name)
        .collect(Collectors.joining(", "));
    return "FlagSet{" +
        "flags=[" + flagsStr +
        "], mask=" + mask +
        '}';
  }


  // --------------------------------------------------------------------------------


  /**
   * A builder for creating a {@link AbstractFlagSet}.
   *
   * @param <E> The type of flag to be held in the {@link AbstractFlagSet}
   * @param <S> The type of the {@link AbstractFlagSet} implementation.
   */
  public static class Builder<E extends Enum<E> & MaskedFlag, S extends AbstractFlagSet<E>> {

    final Class<E> type;
    final EnumSet<E> enumSet;
    final Function<EnumSet<E>, S> constructor;

    protected Builder(final Class<E> type,
                      final Function<EnumSet<E>, S> constructor) {
      this.type = type;
      this.enumSet = EnumSet.noneOf(type);
      this.constructor = constructor;
    }

    /**
     * Replaces any flags already set in the builder with the contents of the passed flags {@link Collection}
     *
     * @param flags The flags to set in the builder.
     * @return this builder instance.
     */
    public Builder<E, S> withFlags(final Collection<E> flags) {
      enumSet.clear();
      if (flags != null) {
        for (E flag : flags) {
          if (flag != null) {
            enumSet.add(flag);
          }
        }
      }
      return this;
    }

    /**
     * @param flags The flags to set in the builder.
     * @return this builder instance.
     */
    @SafeVarargs
    public final Builder<E, S> withFlags(final E... flags) {
      enumSet.clear();
      if (flags != null) {
        for (E flag : flags) {
          if (flag != null) {
            if (!type.equals(flag.getClass())) {
              throw new IllegalArgumentException("Unexpected type " + flag.getClass());
            }
            enumSet.add(flag);
          }
        }
      }
      return this;
    }

    /**
     * Sets a single flag in the builder.
     *
     * @param flag The flag to set in the builder.
     * @return this builder instance.
     */
    public Builder<E, S> setFlag(final E flag) {
      if (flag != null) {
        enumSet.add(flag);
      }
      return this;
    }

    /**
     * Clears any flags already set in this {@link Builder}
     *
     * @return this builder instance.
     */
    public Builder<E, S> clear() {
      enumSet.clear();
      return this;
    }

    /**
     * Build the {@link DbiFlagSet}
     *
     * @return A
     */
    public S build() {
      return constructor.apply(enumSet);
    }
  }
}

