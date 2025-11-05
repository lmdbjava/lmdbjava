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
  public DbiBuilderStage2<T> setDbName(final String name) {
    // Null name is allowed so no null check
    final byte[] nameBytes = name == null
        ? null
        : name.getBytes(Env.DEFAULT_NAME_CHARSET);
    return setDbName(nameBytes);
  }

  /**
   * Create the {@link Dbi} with the passed name in byte[] form.
   * @param name The name of the database in byte form.
   * @return The next builder stage.
   */
  public DbiBuilderStage2<T> setDbName(final byte[] name) {
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
   * {@link DbiBuilder#setDbName(String)} or {@link DbiBuilder#setDbName(byte[])}.
   * </p>
   * <p>Note: The 'unnamed database' is used by LMDB to store the names of named databases, with
   * the database name being the key. Use of the unnamed database is intended for simple applications
   * with only one database.</p>
   * @return The next builder stage.
   */
  public DbiBuilderStage2<T> withoutDbName() {
    return setDbName((byte[]) null);
  }


  // --------------------------------------------------------------------------------


  /**
   * Intermediate builder stage for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static class DbiBuilderStage2<T> {

    private final DbiBuilder<T> dbiBuilder;

    private java.util.Comparator<T> customComparator;
    private ComparatorType comparatorType;

    private DbiBuilderStage2(final DbiBuilder<T> dbiBuilder) {
      this.dbiBuilder = dbiBuilder;
    }

    /**
     * <p>
     * This is the <strong>default</strong> choice when it comes to choosing a comparator.
     * If you are not sure of the implications of the other methods then use this one as it
     * is likely what you want and also probably the most performant.
     * </p>
     * <p>
     * With this option, {@link CursorIterable} will make use of the LmdbJava's default
     * Java-side comparators when comparing iteration keys to the start/stop keys.
     * LMDB will use its own comparator for controlling insertion order in the database.
     * The two comparators are functionally identical.
     * </p>
     * <p>
     * This option may be slightly more performant than when using
     * {@link DbiBuilderStage2#withNativeComparator()} which calls down to LMDB for ALL
     * comparison operations.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link DbiBuilderStage2#withNativeComparator()},
     * {@link DbiBuilderStage2#withDefaultComparator()} or
     * {@link DbiBuilderStage2#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withDefaultComparator() {
      this.comparatorType = ComparatorType.DEFAULT;
      return new DbiBuilderStage3<>(this);
    }

    /**
     * <p>
     * With this option, {@link CursorIterable} will call down to LMDB's {@code mdb_cmp} method when
     * comparing iteration keys to start/stop keys. This ensures LmdbJava is comparing start/stop
     * keys using the same comparator that is used for insertion order into the db.
     * </p>
     * <p>
     * This option may be slightly less performant than when using
     * {@link DbiBuilderStage2#withDefaultComparator()} as it needs to call down
     * to LMDB to perform the comparisons, however it guarantees that {@link CursorIterable}
     * key comparison matches LMDB key comparison.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link DbiBuilderStage2#withNativeComparator()},
     * {@link DbiBuilderStage2#withDefaultComparator()} or
     * {@link DbiBuilderStage2#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withNativeComparator() {
      this.comparatorType = ComparatorType.NATIVE;
      return new DbiBuilderStage3<>(this);
    }


    /**
     * Provide a java-side {@link Comparator} that LMDB will call back to for <strong>all</strong>
     * comparison operations.
     * Therefore, it will be called by LMDB to manage database insertion/iteration order.
     * It will also be used for {@link CursorIterable} start/stop key comparisons.
     * <p>
     * It can be useful if you need to sort your database using some other method,
     * e.g. signed keys or case-insensitive order.
     * Note, if you need keys stored in reverse order, see {@link DbiFlags#MDB_REVERSEKEY}
     * and {@link DbiFlags#MDB_REVERSEDUP}.
     * </p>
     * <p>
     * As this requires LMDB to call back to java, this will be less performant than using LMDB's
     * default comparators, but allows for total control over the order in which entries
     * are stored in the database.
     * </p>
     *
     * @param comparator for all key comparison operations.
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withCallbackComparator(final Comparator<T> comparator) {
      this.customComparator = Objects.requireNonNull(comparator);
      this.comparatorType = ComparatorType.CALLBACK;
      return new DbiBuilderStage3<>(this);
    }

    /**
     * <hr>
     * <p>
     * <strong>WARNING</strong>: Only use this if you fully understand the risks and implications.
     * </p>
     * <hr>
     * <p>
     * With this option, {@link CursorIterable} will make use of the passed comparator for
     * comparing iteration keys to start/stop keys. It has <strong>NO</strong> bearing on the
     * insert/iteration order of the database (which is controlled by LMDB's own comparators).
     * </p>
     * <p>
     * It is <strong>vital</strong> that this comparator is functionally identical to the one
     * used internally in LMDB for insertion/iteration order, else you will see unexpected behaviour
     * when using {@link CursorIterable}.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link DbiBuilderStage2#withNativeComparator()},
     * {@link DbiBuilderStage2#withDefaultComparator()} or
     * {@link DbiBuilderStage2#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @param comparator The comparator to use with {@link CursorIterable}.
     * @return The next builder stage.
     */
    public DbiBuilderStage3<T> withIteratorComparator(final Comparator<T> comparator) {
      this.customComparator = Objects.requireNonNull(comparator);
      this.comparatorType = ComparatorType.ITERATOR;
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
    private final AbstractFlagSet.Builder<DbiFlags, DbiFlagSet> flagSetBuilder = DbiFlagSet.builder();
    private Txn<T> txn = null;

    private DbiBuilderStage3(DbiBuilderStage2<T> dbiBuilderStage2) {
      this.dbiBuilderStage2 = dbiBuilderStage2;
    }

    /**
     * <p>
     * Apply all the dbi flags supplied in dbiFlags.
     * </p>
     * <p>
     * Clears all flags currently set by previous calls to
     * {@link DbiBuilderStage3#setDbiFlags(Collection)},
     * {@link DbiBuilderStage3#setDbiFlags(DbiFlags...)}
     * or {@link DbiBuilderStage3#addDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlags to open the database with.
     *                 A null {@link Collection} will just clear all set flags.
     *                 Null items are ignored.
     */
    public DbiBuilderStage3<T> setDbiFlags(final Collection<DbiFlags> dbiFlags) {
      flagSetBuilder.clear();
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
     * Clears all flags currently set by previous calls to
     * {@link DbiBuilderStage3#setDbiFlags(Collection)},
     * {@link DbiBuilderStage3#setDbiFlags(DbiFlags...)}
     * or {@link DbiBuilderStage3#addDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlags to open the database with.
     *                 A null array will just clear all set flags.
     *                 Null items are ignored.
     */
    public DbiBuilderStage3<T> setDbiFlags(final DbiFlags... dbiFlags) {
      flagSetBuilder.clear();
      if (dbiFlags != null) {
        Arrays.stream(dbiFlags)
            .filter(Objects::nonNull)
            .forEach(this.flagSetBuilder::setFlag);
      }
      return this;
    }

    /**
     * <p>
     * Apply all the dbi flags supplied in dbiFlags.
     * </p>
     * <p>
     * Clears all flags currently set by previous calls to
     * {@link DbiBuilderStage3#setDbiFlags(Collection)},
     * {@link DbiBuilderStage3#setDbiFlags(DbiFlags...)}
     * or {@link DbiBuilderStage3#addDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlagSet to open the database with.
     *                 A null value will just clear all set flags.
     */
    public DbiBuilderStage3<T> setDbiFlags(final DbiFlagSet dbiFlagSet) {
      flagSetBuilder.clear();
      if (dbiFlagSet != null) {
        this.flagSetBuilder.withFlags(dbiFlagSet.getFlags());
      }
      return this;
    }

    /**
     * Adds a dbiFlag to those flags already added to this builder by
     * {@link DbiBuilderStage3#setDbiFlags(DbiFlags...)},
     * {@link DbiBuilderStage3#setDbiFlags(Collection)}
     * or {@link DbiBuilderStage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlag to add to any existing flags. A null value is a no-op.
     * @return this builder instance.
     */
    public DbiBuilderStage3<T> addDbiFlag(final DbiFlags dbiFlag) {
      this.flagSetBuilder.setFlag(dbiFlag);
      return this;
    }

    /**
     * Adds a dbiFlag to those flags already added to this builder by
     * {@link DbiBuilderStage3#setDbiFlags(DbiFlags...)},
     * {@link DbiBuilderStage3#setDbiFlags(Collection)}
     * or {@link DbiBuilderStage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlagSet to add to any existing flags. A null value is a no-op.
     * @return this builder instance.
     */
    public DbiBuilderStage3<T> addDbiFlags(final DbiFlagSet dbiFlagSet) {
      if (dbiFlagSet != null) {
        flagSetBuilder.setFlags(dbiFlagSet.getFlags());
      }
      return this;
    }

    /**
     * Use the supplied transaction to open the {@link Dbi}.
     * <p>
     * The caller MUST commit the transaction after calling {@link DbiBuilderStage3#open()},
     * in order to retain the <code>Dbi</code> in the <code>Env</code>.
     * </p>
     * <p>
     * If you don't call this method to supply a {@link Txn}, a {@link Txn} will be opened for the purpose
     * of creating and opening the {@link Dbi}, then closed. Therefore, if you already have a transaction
     * open, you should supply that to avoid one blocking the other.
     * </p>
     *
     * @param txn transaction to use (required; not closed). If the {@link Env} was opened
     *            with the {@link EnvFlags#MDB_RDONLY_ENV} flag, the {@link Txn} can be read-only,
     *            else it needs to be a read/write {@link Txn}.
     * @return this builder instance.
     */
    public DbiBuilderStage3<T> setTxn(final Txn<T> txn) {
      this.txn = Objects.requireNonNull(txn);
      return this;
    }

    /**
     * Construct and open the {@link Dbi}.
     * <p>
     * If a {@link Txn} was supplied to the builder, it is the callers responsibility to
     * commit and close the txn upon return from this method, else the created DB won't be retained.
     * </p>
     *
     * @return A newly constructed and opened {@link Dbi}.
     */
    public Dbi<T> open() {
      final DbiBuilder<T> dbiBuilder = dbiBuilderStage2.dbiBuilder;
      if (txn != null) {
        return open(txn, dbiBuilder);
      } else {
        try (final Txn<T> txn = getTxn(dbiBuilder)) {
          final Dbi<T> dbi = open(txn, dbiBuilder);
          // even RO Txns require a commit to retain Dbi in Env
          txn.commit();
          return dbi;
        }
      }
    }

    private Txn<T> getTxn(final DbiBuilder<T> dbiBuilder) {
      return dbiBuilder.readOnly
          ? dbiBuilder.env.txnRead()
          : dbiBuilder.env.txnWrite();
    }

    private Comparator<T> getComparator(final DbiBuilder<T> dbiBuilder,
                                        final ComparatorType comparatorType,
                                        final DbiFlagSet dbiFlagSet) {
      Comparator<T> comparator = null;
      switch (comparatorType) {
        case DEFAULT:
          // Get the appropriate default CursorIterable comparator based on the DbiFlags,
          // e.g. MDB_INTEGERKEY may benefit from an optimised comparator.
          comparator = dbiBuilder.proxy.getComparator(dbiFlagSet);
          break;
        case CALLBACK:
        case ITERATOR:
          comparator = dbiBuilderStage2.customComparator;
          break;
        case NATIVE:
          break;
        default:
          throw new IllegalStateException("Unexpected comparatorType " + comparatorType);
      }
      return comparator;
    }

    private Dbi<T> open(final Txn<T> txn,
                        final DbiBuilder<T> dbiBuilder) {
      final DbiFlagSet dbiFlagSet = flagSetBuilder.build();
      final ComparatorType comparatorType = dbiBuilderStage2.comparatorType;
      final Comparator<T> comparator = getComparator(dbiBuilder, comparatorType, dbiFlagSet);
      final boolean useNativeCallback = comparatorType == ComparatorType.CALLBACK;
      return new Dbi<>(
          dbiBuilder.env,
          txn,
          dbiBuilder.name,
          comparator,
          useNativeCallback,
          dbiBuilder.proxy,
          dbiFlagSet);
    }
  }


  // --------------------------------------------------------------------------------


  private enum ComparatorType {
    /**
     * Default Java comparator for {@link CursorIterable} KeyRange testing,
     * LMDB comparator for insertion/iteration order.
     */
    DEFAULT,
    /**
     * Use LMDB native comparator for everything.
     */
    NATIVE,
    /**
     * Use the supplied custom Java-side comparator for everything.
     */
    CALLBACK,
    /**
     * Use the supplied custom Java-side comparator for {@link CursorIterable} KeyRange testing,
     * LMDB comparator for insertion/iteration order.
     */
    ITERATOR,
    ;
  }
}
