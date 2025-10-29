/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Encapsulates an immutable set of flags and the associated bit mask for the flags in the set.
 *
 * @param <T>
 */
public abstract class AbstractFlagSet<T extends Enum<T> & MaskedFlag> implements FlagSet<T> {

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
  @Override
  public int getMask() {
    return mask;
  }

  /**
   * @return All flags in the set.
   */
  @Override
  public Set<T> getFlags() {
    return flags;
  }

  /**
   * @return True if flag has been set, i.e. is contained in this set.
   */
  @Override
  public boolean isSet(final T flag) {
    // Probably cheaper to compare the masks than to use EnumSet.contains()
    return flag != null
        && MaskedFlag.isSet(mask, flag);
  }

  /**
   * @return The number of flags in this set.
   */
  @Override
  public int size() {
    return flags.size();
  }

  /**
   * @return True if this set is empty.
   */
  @Override
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
    return FlagSet.equals(this, object);
  }

  @Override
  public int hashCode() {
    return Objects.hash(flags, mask);
  }

  @Override
  public String toString() {
    return FlagSet.asString(this);
  }


  // --------------------------------------------------------------------------------


  static abstract class AbstractSingleFlagSet<T extends Enum<T> & MaskedFlag> implements FlagSet<T> {

    private final T flag;
    // Only holding this for iterator() and getFlags() so make it lazy.
    private EnumSet<T> enumSet;

    public AbstractSingleFlagSet(final T flag) {
      this.flag = Objects.requireNonNull(flag);
    }

    @Override
    public int getMask() {
      return flag.getMask();
    }

    @Override
    public Set<T> getFlags() {
      if (enumSet == null) {
        return initSet();
      } else {
        return this.enumSet;
      }
    }

    @Override
    public boolean isSet(final T flag) {
      return this.flag == flag;
    }

    @Override
    public boolean areAnySet(FlagSet<T> flags) {
      if (flags == null) {
        return false;
      } else {
        return flags.isSet(this.flag);
      }
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public Iterator<T> iterator() {
      if (enumSet == null) {
        return initSet().iterator();
      } else {
        return this.enumSet.iterator();
      }
    }

    @Override
    public String toString() {
      return FlagSet.asString(this);
    }

    @Override
    public boolean equals(Object object) {
      return FlagSet.equals(this, object);
    }

    @Override
    public int hashCode() {
      return Objects.hash(flag, getFlags());
    }

    private Set<T> initSet() {
      final EnumSet<T> set = EnumSet.of(this.flag);
      this.enumSet = set;
      return set;
    }
  }


  // --------------------------------------------------------------------------------


  static class AbstractEmptyFlagSet<T extends MaskedFlag> implements FlagSet<T> {

    @Override
    public int getMask() {
      return MaskedFlag.EMPTY_MASK;
    }

    @Override
    public Set<T> getFlags() {
      return Collections.emptySet();
    }

    @Override
    public boolean isSet(final T flag) {
      return false;
    }

    @Override
    public boolean areAnySet(final FlagSet<T> flags) {
      return false;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Iterator<T> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return FlagSet.asString(this);
    }

    @Override
    public boolean equals(Object object) {
      return FlagSet.equals(this, object);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getMask(), getFlags());
    }
  }


  // --------------------------------------------------------------------------------


  /**
   * A builder for creating a {@link AbstractFlagSet}.
   *
   * @param <E> The type of flag to be held in the {@link AbstractFlagSet}
   * @param <S> The type of the {@link AbstractFlagSet} implementation.
   */
  public static class Builder<E extends Enum<E> & MaskedFlag, S extends FlagSet<E>> {

    final Class<E> type;
    final EnumSet<E> enumSet;
    final Function<EnumSet<E>, S> constructor;
    final Function<E, S> singletonSetConstructor;
    final Supplier<S> emptySetSupplier;

    protected Builder(final Class<E> type,
                      final Function<EnumSet<E>, S> constructor,
                      final Function<E, S> singletonSetConstructor,
                      final Supplier<S> emptySetSupplier) {
      this.type = type;
      this.enumSet = EnumSet.noneOf(type);
      this.constructor = Objects.requireNonNull(constructor);
      this.singletonSetConstructor = Objects.requireNonNull(singletonSetConstructor);
      this.emptySetSupplier = Objects.requireNonNull(emptySetSupplier);
    }

    /**
     * Replaces any flags already set in the builder with the contents of the passed flags {@link Collection}
     *
     * @param flags The flags to set in the builder.
     * @return this builder instance.
     */
    public Builder<E, S> withFlags(final Collection<E> flags) {
      clear();
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
      clear();
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
      final int size = enumSet.size();
      if (size == 0) {
        return emptySetSupplier.get();
      } else if (size == 1) {
        return singletonSetConstructor.apply(enumSet.stream().findFirst().get());
      } else {
        return constructor.apply(enumSet);
      }
    }
  }
}
