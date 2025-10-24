package org.lmdbjava;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

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
   */
  public RequireComparator<T> withDbName(final String name) {
    // Null name is allowed so no null check
    final byte[] nameBytes = name == null
        ? null
        : name.getBytes(StandardCharsets.UTF_8);
    return withDbName(nameBytes);
  }

  /**
   * Create the {@link Dbi} with the passed name in byte[] form.
   */
  public RequireComparator<T> withDbName(final byte[] name) {
    // Null name is allowed so no null check
    this.name = name;
    return new RequireComparator<>(this);
  }

  /**
   * <p>
   * Create the {@link Dbi} without a name.
   * </p>
   * <p>
   * Equivalent to passing null to
   * {@link DbiBuilder#withDbName(String)} or {@link DbiBuilder#withDbName(byte[])}.
   * </p>
   */
  public RequireComparator<T> withoutDbName() {
    return withDbName((byte[]) null);
  }


  // --------------------------------------------------------------------------------


  /**
   * Intermediate builder stage for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static class RequireComparator<T> {

    private final DbiBuilder<T> dbiBuilder;

    private Comparator<T> comparator;
    private boolean useNativeCallback;

    private RequireComparator(final DbiBuilder<T> dbiBuilder) {
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
     * {@link RequireComparator#withDefaultJavaComparator()} as it need to call down
     * to LMDB to perform the comparisons, however it guarantees that {@link CursorIterable}
     * key comparison matches LMDB key comparison.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link RequireComparator#withNativeComparator()},
     * {@link RequireComparator#withDefaultJavaComparator()} or
     * {@link RequireComparator#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @return this builder instance.
     */
    public FinalStage<T> withNativeComparator() {
      this.comparator = null;
      this.useNativeCallback = false;
      return new FinalStage<>(this);
    }

    /**
     * <p>
     * {@link CursorIterable} will make use of the default Java-side comparators when
     * comparing entries to start/stop keys.
     * </p>
     * <p>
     * This option may be slightly more performant than when using
     * {@link RequireComparator#withNativeComparator()} but it relies on the default comparator
     * in LmdbJava behaving identically to the comparator in LMDB.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link RequireComparator#withNativeComparator()},
     * {@link RequireComparator#withDefaultJavaComparator()} or
     * {@link RequireComparator#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @return this builder instance.
     */
    public FinalStage<T> withDefaultJavaComparator() {
      this.comparator = dbiBuilder.proxy.getUnsignedComparator();
      this.useNativeCallback = false;
      return new FinalStage<>(this);
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
     * @return this builder instance.
     */
    public FinalStage<T> withCallbackComparator(final Comparator<T> comparator) {
      this.comparator = Objects.requireNonNull(comparator);
      this.useNativeCallback = true;
      return new FinalStage<>(this);
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
     * This option may be slightly less performant than when using
     * {@link RequireComparator#withDefaultJavaComparator()} as it need to call down
     * to LMDB to perform the comparisons, however it guarantees that {@link CursorIterable}
     * key comparison matches LMDB key comparison.
     * </p>
     * <p>
     * If you do not intend to use {@link CursorIterable} then it doesn't matter whether
     * you choose {@link RequireComparator#withNativeComparator()},
     * {@link RequireComparator#withDefaultJavaComparator()} or
     * {@link RequireComparator#withIteratorComparator(Comparator)} as these comparators will
     * never be used.
     * </p>
     *
     * @param comparator The comparator to use with {@link CursorIterable}.
     * @return this builder instance.
     */
    public FinalStage<T> withIteratorComparator(final Comparator<T> comparator) {
      this.comparator = Objects.requireNonNull(comparator);
      this.useNativeCallback = false;
      return new FinalStage<>(this);
    }
  }


  // --------------------------------------------------------------------------------


  /**
   * Final stage builder for constructing a {@link Dbi}.
   *
   * @param <T> buffer type
   */
  public static class FinalStage<T> {

    private final RequireComparator<T> requireComparator;
    private Set<DbiFlags> dbiFlags = null;
    private Txn<T> txn = null;

    private FinalStage(RequireComparator<T> requireComparator) {
      this.requireComparator = requireComparator;
    }

    private void initDbiFlags() {
      if (dbiFlags == null) {
        dbiFlags = EnumSet.noneOf(DbiFlags.class);
      }
    }

    /**
     * <p>
     * Apply all the dbi flags supplied in dbiFlags.
     * </p>
     * <p>
     * Replaces any flags applies in previous calls to
     * {@link FinalStage#withDbiFlags(Collection)}, {@link FinalStage#withDbiFlags(DbiFlags...)}
     * or {@link FinalStage#addDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlags to open the database with.
     */
    public FinalStage<T> withDbiFlags(final Collection<DbiFlags> dbiFlags) {
      initDbiFlags();
      if (dbiFlags != null) {
        this.dbiFlags.addAll(dbiFlags);
      }
      return this;
    }

    /**
     * <p>
     * Apply all the dbi flags supplied in dbiFlags.
     * </p>
     * <p>
     * Replaces any flags applies in previous calls to
     * {@link FinalStage#withDbiFlags(Collection)}, {@link FinalStage#withDbiFlags(DbiFlags...)}
     * or {@link FinalStage#addDbiFlag(DbiFlags)}.
     * </p>
     *
     * @param dbiFlags to open the database with.
     */
    public FinalStage<T> withDbiFlags(final DbiFlags... dbiFlags) {
      initDbiFlags();
      if (dbiFlags != null) {
        Arrays.stream(dbiFlags)
            .filter(Objects::nonNull)
            .forEach(this.dbiFlags::add);
      }
      return this;
    }

    /**
     * Adds dbiFlag to those flags already added to this builder.
     *
     * @param dbiFlag to open the database with.
     * @return this builder instance.
     */
    public FinalStage<T> addDbiFlag(final DbiFlags dbiFlag) {
      initDbiFlags();
      if (dbiFlags != null) {
        this.dbiFlags.add(dbiFlag);
      }
      return this;
    }

    /**
     * Use the supplied transaction to open the {@link Dbi}.
     * <p>
     * The caller must commit the transaction after calling {@link FinalStage#open()}
     * in order to retain the <code>Dbi</code> in the <code>Env</code>.
     * </p>
     *
     * @param txn transaction to use (required; not closed)
     * @return this builder instance.
     */
    public FinalStage<T> withTxn(final Txn<T> txn) {
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
      final DbiBuilder<T> dbiBuilder = requireComparator.dbiBuilder;
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
      final DbiFlags[] dbiFlagsArr = dbiFlags != null && !dbiFlags.isEmpty()
          ? this.dbiFlags.toArray(new DbiFlags[0])
          : null;

      return new Dbi<>(
          dbiBuilder.env,
          txn,
          dbiBuilder.name,
          requireComparator.comparator,
          requireComparator.useNativeCallback,
          dbiBuilder.proxy,
          dbiFlagsArr);
    }
  }
}
