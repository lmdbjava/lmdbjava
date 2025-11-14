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
public final class DbiBuilder<T> {

  private final Env<T> env;
  private final BufferProxy<T> proxy;
  private final boolean readOnly;
  private byte[] name;

  DbiBuilder(final Env<T> env, final BufferProxy<T> proxy, final boolean readOnly) {
    this.env = Objects.requireNonNull(env);
    this.proxy = Objects.requireNonNull(proxy);
    this.readOnly = readOnly;
  }

  /**
   * Create the {@link Dbi} with the passed name.
   *
   * <p>The name will be converted into bytes using {@link StandardCharsets#UTF_8}.
   *
   * @param name The name of the database or null for the unnamed database (see also {@link
   *     DbiBuilder#withoutDbName()})
   * @return The next builder stage.
   */
  public Stage2<T> setDbName(final String name) {
    // Null name is allowed so no null check
    final byte[] nameBytes = name == null ? null : name.getBytes(Env.DEFAULT_NAME_CHARSET);
    return setDbName(nameBytes);
  }

  /**
   * Create the {@link Dbi} with the passed name in byte[] form.
   *
   * @param name The name of the database in byte form.
   * @return The next builder stage.
   */
  public Stage2<T> setDbName(final byte[] name) {
    // Null name is allowed so no null check
    this.name = name;
    return new Stage2<>(this);
  }

  /**
   * Create the {@link Dbi} without a name.
   *
   * <p>Equivalent to passing null to {@link DbiBuilder#setDbName(String)} or {@link
   * DbiBuilder#setDbName(byte[])}.
   *
   * <p>Note: The 'unnamed database' is used by LMDB to store the names of named databases, with the
   * database name being the key. Use of the unnamed database is intended for simple applications
   * with only one database.
   *
   * @return The next builder stage.
   */
  public Stage2<T> withoutDbName() {
    return setDbName((byte[]) null);
  }

  /**
   * Intermediate builder stage for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static final class Stage2<T> {

    private final DbiBuilder<T> dbiBuilder;

    private ComparatorFactory<T> comparatorFactory;
    private ComparatorType comparatorType;

    private Stage2(final DbiBuilder<T> dbiBuilder) {
      this.dbiBuilder = dbiBuilder;
    }

    /**
     * This is the <strong>default</strong> choice when it comes to choosing a comparator. If you
     * are not sure of the implications of the other methods then use this one as it is likely what
     * you want and also probably the most performant.
     *
     * <p>With this option, {@link CursorIterable} will make use of the LmdbJava's default Java-side
     * comparators when comparing iteration keys to the start/stop keys. LMDB will use its own
     * comparator for controlling insertion order in the database. The two comparators are
     * functionally identical.
     *
     * <p>This option may be slightly more performant than when using {@link
     * Stage2#withNativeComparator()} which calls down to LMDB for ALL comparison operations.
     *
     * <p>If you do not intend to use {@link CursorIterable} then it doesn't matter whether you
     * choose {@link Stage2#withNativeComparator()}, {@link Stage2#withDefaultComparator()} or
     * {@link Stage2#withIteratorComparator(ComparatorFactory)} as these comparators will never be
     * used.
     *
     * @return The next builder stage.
     */
    public Stage3<T> withDefaultComparator() {
      this.comparatorType = ComparatorType.DEFAULT;
      return new Stage3<>(this);
    }

    /**
     * With this option, {@link CursorIterable} will call down to LMDB's {@code mdb_cmp} method when
     * comparing iteration keys to start/stop keys. This ensures LmdbJava is comparing start/stop
     * keys using the same comparator that is used for insertion order into the db.
     *
     * <p>This option may be slightly less performant than when using {@link
     * Stage2#withDefaultComparator()} as it needs to call down to LMDB to perform the comparisons,
     * however it guarantees that {@link CursorIterable} key comparison matches LMDB key comparison.
     *
     * <p>If you do not intend to use {@link CursorIterable} then it doesn't matter whether you
     * choose {@link Stage2#withNativeComparator()}, {@link Stage2#withDefaultComparator()} or
     * {@link Stage2#withIteratorComparator(ComparatorFactory)} as these comparators will never be
     * used.
     *
     * @return The next builder stage.
     */
    public Stage3<T> withNativeComparator() {
      this.comparatorType = ComparatorType.NATIVE;
      return new Stage3<>(this);
    }

    /**
     * Provide a java-side {@link Comparator} that LMDB will call back to for <strong>all</strong>
     * comparison operations. Therefore, it will be called by LMDB to manage database
     * insertion/iteration order. It will also be used for {@link CursorIterable} start/stop key
     * comparisons.
     *
     * <p>It can be useful if you need to sort your database using some other method, e.g. signed
     * keys or case-insensitive order. Note, if you need keys stored in reverse order, see {@link
     * DbiFlags#MDB_REVERSEKEY} and {@link DbiFlags#MDB_REVERSEDUP}.
     *
     * <p>As this requires LMDB to call back to java, this will be less performant than using LMDB's
     * default comparators, but allows for total control over the order in which entries are stored
     * in the database.
     *
     * @param comparatorFactory A factory to create a comparator. {@link
     *     ComparatorFactory#create(DbiFlagSet)} will be called once during the initialisation of
     *     the {@link Dbi}. It must not return null.
     * @return The next builder stage.
     */
    public Stage3<T> withCallbackComparator(final ComparatorFactory<T> comparatorFactory) {
      this.comparatorFactory = Objects.requireNonNull(comparatorFactory);
      this.comparatorType = ComparatorType.CALLBACK;
      return new Stage3<>(this);
    }

    /**
     * <strong>WARNING</strong>: Only use this if you fully understand the risks and implications.
     * <hr>
     *
     * <p>With this option, {@link CursorIterable} will make use of the passed comparator for
     * comparing iteration keys to start/stop keys. It has <strong>NO</strong> bearing on the
     * insert/iteration order of the database (which is controlled by LMDB's own comparators).
     *
     * <p>It is <strong>vital</strong> that this comparator is functionally identical to the one
     * used internally in LMDB for insertion/iteration order, else you will see unexpected behaviour
     * when using {@link CursorIterable}.
     *
     * <p>If you do not intend to use {@link CursorIterable} then it doesn't matter whether you
     * choose {@link Stage2#withNativeComparator()}, {@link Stage2#withDefaultComparator()} or
     * {@link Stage2#withIteratorComparator(ComparatorFactory)} as these comparators will never be
     * used.
     *
     * @param comparatorFactory The comparator to use with {@link CursorIterable}. {@link
     *     ComparatorFactory#create(DbiFlagSet)} will be called once during the initialisation of
     *     the {@link Dbi}. It must not return null.
     * @return The next builder stage.
     */
    public Stage3<T> withIteratorComparator(final ComparatorFactory<T> comparatorFactory) {
      this.comparatorFactory = Objects.requireNonNull(comparatorFactory);
      this.comparatorType = ComparatorType.ITERATOR;
      return new Stage3<>(this);
    }
  }

  /**
   * Final stage builder for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static final class Stage3<T> {

    private final Stage2<T> stage2;
    private final AbstractFlagSet.Builder<DbiFlags, DbiFlagSet> flagSetBuilder =
        DbiFlagSet.builder();
    private Txn<T> txn = null;

    private Stage3(Stage2<T> stage2) {
      this.stage2 = stage2;
    }

    /**
     * Apply all the dbi flags supplied in dbiFlags.
     *
     * <p>Clears all flags currently set by previous calls to {@link
     * Stage3#setDbiFlags(Collection)}, {@link Stage3#setDbiFlags(DbiFlags...)} or {@link
     * Stage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlags to open the database with. A null {@link Collection} will just clear all set
     *     flags. Null items are ignored.
     * @return This builder instance.
     */
    public Stage3<T> setDbiFlags(final Collection<DbiFlags> dbiFlags) {
      flagSetBuilder.clear();
      if (dbiFlags != null) {
        dbiFlags.stream().filter(Objects::nonNull).forEach(this.flagSetBuilder::addFlag);
      }
      return this;
    }

    /**
     * Apply all the dbi flags supplied in dbiFlags.
     *
     * <p>Clears all flags currently set by previous calls to {@link
     * Stage3#setDbiFlags(Collection)}, {@link Stage3#setDbiFlags(DbiFlags...)} or {@link
     * Stage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlags to open the database with. A null array will just clear all set flags. Null
     *     items are ignored.
     * @return This builder instance.
     */
    public Stage3<T> setDbiFlags(final DbiFlags... dbiFlags) {
      flagSetBuilder.clear();
      if (dbiFlags != null) {
        Arrays.stream(dbiFlags).filter(Objects::nonNull).forEach(this.flagSetBuilder::addFlag);
      }
      return this;
    }

    /**
     * Apply all the dbi flags supplied in dbiFlags.
     *
     * <p>Clears all flags currently set by previous calls to {@link
     * Stage3#setDbiFlags(Collection)}, {@link Stage3#setDbiFlags(DbiFlags...)} or {@link
     * Stage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlagSet to open the database with. A null value will just clear all set flags.
     * @return This builder instance.
     */
    public Stage3<T> setDbiFlags(final DbiFlagSet dbiFlagSet) {
      flagSetBuilder.clear();
      if (dbiFlagSet != null) {
        this.flagSetBuilder.setFlags(dbiFlagSet.getFlags());
      }
      return this;
    }

    /**
     * Adds a dbiFlag to those flags already added to this builder by {@link
     * Stage3#setDbiFlags(DbiFlags...)}, {@link Stage3#setDbiFlags(Collection)} or {@link
     * Stage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlag to add to any existing flags. A null value is a no-op.
     * @return this builder instance.
     */
    public Stage3<T> addDbiFlag(final DbiFlags dbiFlag) {
      this.flagSetBuilder.addFlag(dbiFlag);
      return this;
    }

    /**
     * Adds a dbiFlag to those flags already added to this builder by {@link
     * Stage3#setDbiFlags(DbiFlags...)}, {@link Stage3#setDbiFlags(Collection)} or {@link
     * Stage3#addDbiFlag(DbiFlags)}.
     *
     * @param dbiFlagSet to add to any existing flags. A null value is a no-op.
     * @return this builder instance.
     */
    public Stage3<T> addDbiFlags(final DbiFlagSet dbiFlagSet) {
      if (dbiFlagSet != null) {
        this.flagSetBuilder.addFlags(dbiFlagSet.getFlags());
      }
      return this;
    }

    /**
     * Use the supplied transaction to open the {@link Dbi}.
     *
     * <p>The caller MUST commit the transaction after calling {@link Stage3#open()}, in order to
     * retain the <code>Dbi</code> in the <code>Env</code>.
     *
     * <p>If you don't call this method to supply a {@link Txn}, a {@link Txn} will be opened for
     * the purpose of creating and opening the {@link Dbi}, then closed. Therefore, if you already
     * have a transaction open, you should supply that to avoid one blocking the other.
     *
     * @param txn transaction to use (required; not closed). If the {@link Env} was opened with the
     *     {@link EnvFlags#MDB_RDONLY_ENV} flag, the {@link Txn} can be read-only, else it needs to
     *     be a read/write {@link Txn}.
     * @return this builder instance.
     */
    public Stage3<T> setTxn(final Txn<T> txn) {
      this.txn = Objects.requireNonNull(txn);
      return this;
    }

    /**
     * Construct and open the {@link Dbi}.
     *
     * <p>If a {@link Txn} was supplied to the builder, it is the callers responsibility to commit
     * and close the txn upon return from this method, else the created DB won't be retained.
     *
     * @return A newly constructed and opened {@link Dbi}.
     */
    public Dbi<T> open() {
      final DbiBuilder<T> dbiBuilder = stage2.dbiBuilder;
      if (txn != null) {
        return openDbi(txn, dbiBuilder);
      } else {
        try (final Txn<T> localTxn = getTxn(dbiBuilder)) {
          final Dbi<T> dbi = openDbi(localTxn, dbiBuilder);
          // even RO Txns require a commit to retain Dbi in Env
          localTxn.commit();
          return dbi;
        }
      }
    }

    private Txn<T> getTxn(final DbiBuilder<T> dbiBuilder) {
      return dbiBuilder.readOnly ? dbiBuilder.env.txnRead() : dbiBuilder.env.txnWrite();
    }

    private Comparator<T> getComparator(
        final DbiBuilder<T> dbiBuilder,
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
          comparator = stage2.comparatorFactory.create(dbiFlagSet);
          Objects.requireNonNull(comparator, "comparatorFactory returned null");
          break;
        case NATIVE:
          break;
        default:
          throw new IllegalStateException("Unexpected comparatorType " + comparatorType);
      }
      return comparator;
    }

    private Dbi<T> openDbi(final Txn<T> txn, final DbiBuilder<T> dbiBuilder) {
      final DbiFlagSet dbiFlagSet = flagSetBuilder.build();
      final ComparatorType comparatorType = stage2.comparatorType;
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

  private enum ComparatorType {
    /**
     * Default Java comparator for {@link CursorIterable} KeyRange testing, LMDB comparator for
     * insertion/iteration order.
     */
    DEFAULT,
    /** Use LMDB native comparator for everything. */
    NATIVE,
    /** Use the supplied custom Java-side comparator for everything. */
    CALLBACK,
    /**
     * Use the supplied custom Java-side comparator for {@link CursorIterable} KeyRange testing,
     * LMDB comparator for insertion/iteration order.
     */
    ITERATOR,
    ;
  }

  /**
   * A factory for creating a {@link Comparator} from a {@link DbiFlagSet}
   *
   * @param <T> The type of buffer that will be compared by the created {@link Comparator}.
   */
  @FunctionalInterface
  public interface ComparatorFactory<T> {

    /**
     * Creates a comparator for the supplied {@link DbiFlagSet}. This will only be called once
     * during the initialisation of the {@link Dbi}.
     *
     * @param dbiFlagSet The flags set on the DB that the returned {@link Comparator} will be used
     *     by. The flags in the set may impact how the returned {@link Comparator} should behave.
     * @return A {@link Comparator} applicable to the passed DB flags.
     */
    Comparator<T> create(final DbiFlagSet dbiFlagSet);
  }
}
