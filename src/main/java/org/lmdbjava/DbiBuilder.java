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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;

/**
 * Staged builder for building a {@link Dbi}
 *
 * @param <T> buffer type
 */
public class DbiBuilder<T> {

  private final Env<T> env;
  private final BufferProxy<T> proxy;
  private final boolean readOnly;
  private byte[] name;

  DbiBuilder(final Env<T> env,
             final BufferProxy<T> proxy,
             final boolean readOnly) {
    this.env = Objects.requireNonNull(env);
    this.proxy = Objects.requireNonNull(proxy);
    this.readOnly = readOnly;
  }

  /**
   * <p>
   * Create the {@link Dbi} with the passed name.
   * </p>
   * <p>
   * The name will be converted into bytes using {@link StandardCharsets#UTF_8}.
   * </p>
   * @param name The name of the database or null for the unnamed database
   *            (see also {@link DbiBuilder#withoutDbName()})
   * @return The next builder stage.
   */
  public DbiBuilderStage2<T> withDbName(final String name) {
    // Null name is allowed so no null check
    final byte[] nameBytes = name == null
        ? null
        : name.getBytes(StandardCharsets.UTF_8);
    return withDbName(nameBytes);
  }

  /**
   * Create the {@link Dbi} with the passed name in byte[] form.
   * @param name The name of the database in byte form.
   * @return The next builder stage.
   */
  public DbiBuilderStage2<T> withDbName(final byte[] name) {
    // Null name is allowed so no null check
    this.name = name;
    return new DbiBuilderStage2<>(this);
  }

  /**
   * <p>
   * Create the {@link Dbi} without a name.
   * </p>
   * <p>
   * Equivalent to passing null to
   * {@link DbiBuilder#withDbName(String)} or {@link DbiBuilder#withDbName(byte[])}.
   * </p>
   * @return The next builder stage.
   */
  public DbiBuilderStage2<T> withoutDbName() {
    return withDbName((byte[]) null);
  }


  // --------------------------------------------------------------------------------


  /**
   * Intermediate builder stage for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static class DbiBuilderStage2<T> {

    private final DbiBuilder<T> dbiBuilder;

    private Comparator<T> comparator;
    private boolean useNativeCallback;

    private DbiBuilderStage2(final DbiBuilder<T> dbiBuilder) {
      this.dbiBuilder = dbiBuilder;
    }

    /**
     * <p>
     * {@link CursorIterable} will call down to LMDB's {@code mdb_cmp} method when
     * comparing entries to start/stop keys. This ensures LmdbJava is comparing start/stop
     * keys using the same comparator that is used for insert order into the db.
     * </p>
     * <p>
     * This option may be slightly less performant than when using
     * {@link DbiBuilderStage2#withDefaultIteratorComparator()} as it need to call down
     * to LMDB to perform the comparisons, however it guarantees that {@link CursorIterable}
     * key comparison matches LMDB key comparison.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link DbiBuilderStage2#withNativeComparator()},
     * {@link DbiBuilderStage2#withDefaultIteratorComparator()} or
     * {@link DbiBuilderStage2#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withNativeComparator() {
      this.comparator = null;
      this.useNativeCallback = false;
      return new DbiBuilderStage3<>(this);
    }

    /**
     * <p>
     * {@link CursorIterable} will make use of the default Java-side comparators when
     * comparing entries to start/stop keys.
     * </p>
     * <p>
     * This option may be slightly more performant than when using
     * {@link DbiBuilderStage2#withNativeComparator()} but it relies on the default comparator
     * in LmdbJava behaving identically to the comparator in LMDB.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link DbiBuilderStage2#withNativeComparator()},
     * {@link DbiBuilderStage2#withDefaultIteratorComparator()} or
     * {@link DbiBuilderStage2#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withDefaultIteratorComparator() {
      this.comparator = dbiBuilder.proxy.getUnsignedComparator();
      this.useNativeCallback = false;
      return new DbiBuilderStage3<>(this);
    }

    /**
     * Provide a java-side {@link Comparator} that LMDB will call back to in order to
     * manage database insertion/iteration order. It will also be used for {@link CursorIterable}
     * start/stop key comparisons.
     * <p>
     * Due to calling back to java, this will be less performant than using LMDB's
     * default comparator, but allows for total control over the order in which entries
     * are stored in the database.
     * </p>
     *
     * @param comparator for all key comparison operations.
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withCallbackIteratorComparator(final Comparator<T> comparator) {
      this.comparator = Objects.requireNonNull(comparator);
      this.useNativeCallback = true;
      return new DbiBuilderStage3<>(this);
    }

    /**
     * <p>
     * {@link CursorIterable} will make use of the passed comparator for
     * comparing entries to start/stop keys. It has NO bearing on the insert/iteration
     * order of the db.
     * </p>
     * <p>
     * <strong>WARNING:</strong> Only call this method if you fully understand the implications
     * of using a comparator for the {@link CursorIterable} start/stop keys that behaves
     * differently to the comparator in LMDB that controls the insert/iteration order.
     * </p>
     * <p>
     * The supplied {@link Comparator} should match the behaviour of LMDB's mdb_cmp comparator.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link DbiBuilderStage2#withNativeComparator()},
     * {@link DbiBuilderStage2#withDefaultIteratorComparator()} or
     * {@link DbiBuilderStage2#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @param comparator The comparator to use with {@link CursorIterable}.
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withIteratorComparator(final Comparator<T> comparator) {
      this.comparator = Objects.requireNonNull(comparator);
      this.useNativeCallback = false;
      return new DbiBuilderStage3<>(this);
    }
  }


  // --------------------------------------------------------------------------------


  /**
   * Final stage builder for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static class DbiBuilderStage3<T> {

    private final DbiBuilderStage2<T> dbiBuilderStage2;
    private final FlagSet.Builder<DbiFlags, DbiFlagSet> flagSetBuilder = DbiFlagSet.builder();
    private Txn<T> txn = null;

    private DbiBuilderStage3(DbiBuilderStage2<T> dbiBuilderStage2) {
      this.dbiBuilderStage2 = dbiBuilderStage2;
    }

    /**
     * <p>
     * Apply all the dbi flags supplied in dbiFlags.
     * </p>
     * <p>
     * Replaces any flags applies in previous calls to
     * {@link DbiBuilderStage3#withDbiFlags(Collection)}, {@link DbiBuilderStage3#withDbiFlags(DbiFlags...)}
     * or {@link DbiBuilderStage3#setDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlags to open the database with.
     */
    public DbiBuilderStage3<T> withDbiFlags(final Collection<DbiFlags> dbiFlags) {
      if (dbiFlags != null) {
        dbiFlags.stream()
                .filter(Objects::nonNull)
                .forEach(dbiFlags::add);
      }
      return this;
    }

    /**
     * <p>
     * Apply all the dbi flags supplied in dbiFlags.
     * </p>
     * <p>
     * Replaces any flags applies in previous calls to
     * {@link DbiBuilderStage3#withDbiFlags(Collection)},
     * {@link DbiBuilderStage3#withDbiFlags(DbiFlags...)}
     * or {@link DbiBuilderStage3#setDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlags to open the database with.
     *                 A null array is a no-op. Null items are ignored.
     */
    public DbiBuilderStage3<T> withDbiFlags(final DbiFlags... dbiFlags) {
      flagSetBuilder.clear();
      if (dbiFlags != null) {
        Arrays.stream(dbiFlags)
            .filter(Objects::nonNull)
            .forEach(this.flagSetBuilder::setFlag);
      }
      return this;
    }

    /**
     * Adds dbiFlag to those flags already added to this builder by
     * {@link DbiBuilderStage3#withDbiFlags(DbiFlags...)},
     * {@link DbiBuilderStage3#withDbiFlags(Collection)}
     * or {@link DbiBuilderStage3#setDbiFlag(DbiFlags)}.
     *
     * @param dbiFlag to open the database with. A null value is a no-op.
     * @return this builder instance.
     */
    public DbiBuilderStage3<T> setDbiFlag(final DbiFlags dbiFlag) {
      this.flagSetBuilder.setFlag(dbiFlag);
      return this;
    }

    /**
     * Use the supplied transaction to open the {@link Dbi}.
     * <p>
     * The caller MUST commit the transaction after calling {@link DbiBuilderStage3#open()},
     * in order to retain the <code>Dbi</code> in the <code>Env</code>.
     * </p>
     *
     * @param txn transaction to use (required; not closed)
     * @return this builder instance.
     */
    public DbiBuilderStage3<T> withTxn(final Txn<T> txn) {
      this.txn = Objects.requireNonNull(txn);
      return this;
    }

    /**
     * Construct and open the {@link Dbi}.
     * <p>
     * If a {@link Txn} was supplied to the builder, it should be committed upon return from
     * this method.
     * </p>
     *
     * @return A newly constructed and opened {@link Dbi}.
     */
    public Dbi<T> open() {
      final DbiBuilder<T> dbiBuilder = dbiBuilderStage2.dbiBuilder;
      if (txn == null) {
        try (final Txn<T> txn = getTxn(dbiBuilder)) {
          return open(txn, dbiBuilder);
        }
      } else {
        return open(txn, dbiBuilder);
      }
    }

    private Txn<T> getTxn(final DbiBuilder<T> dbiBuilder) {
      return dbiBuilder.readOnly
          ? dbiBuilder.env.txnRead()
          : dbiBuilder.env.txnWrite();
    }

    private Dbi<T> open(final Txn<T> txn,
                        final DbiBuilder<T> dbiBuilder) {
      final DbiFlagSet dbiFlagSet = flagSetBuilder.build();

      return new Dbi<>(
          dbiBuilder.env,
          txn,
          dbiBuilder.name,
          dbiBuilderStage2.comparator,
          dbiBuilderStage2.useNativeCallback,
          dbiBuilder.proxy,
          dbiFlagSet);
    }
  }
}
