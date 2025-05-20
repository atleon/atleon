package io.atleon.core;

import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Configures how to deduplicate data items.
 *
 * @param <T> The type of data item being deduplicated
 */
public interface Deduplication<T> {

    /**
     * Creates a new Deduplication using Object equality for duplicate detection, emitting the
     * first received item for (and at the end of) any given deduplication sample.
     */
    static <T> Deduplication<T> identity() {
        return emitFirst(Function.identity());
    }

    /**
     * Creates a new Deduplication using the provided key extractor {@link Function} for
     * deduplicate detection, emitting the first received item for (and at the end of) any given
     * deduplication sample.
     */
    static <T> Deduplication<T> emitFirst(Function<? super T, ?> keyExtractor) {
        return composed(keyExtractor, (first, __) -> first);
    }

    /**
     * Creates a new Deduplication using the provided key extractor {@link Function} for
     * deduplicate detection, emitting the last received item for (and at the end of) any given
     * deduplication sample.
     */
    static <T> Deduplication<T> emitLast(Function<? super T, ?> keyExtractor) {
        return composed(keyExtractor, (__, second) -> second);
    }

    /**
     * @param keyExtractor Function that extracts key used as identifier of duplicate data
     * @param reducer      Binary operator used to reduce duplicate elements
     * @param <T>          The type of elements deduplicated by this {@link Deduplication}
     * @return A new, composed {@link Deduplication}
     */
    static <T> Deduplication<T> composed(Function<? super T, ?> keyExtractor, BinaryOperator<T> reducer) {
        return new Composed<>(keyExtractor, reducer);
    }

    /**
     * Returns the key on which to implement deduplication
     *
     * @param t The data item that deduplication is being enforced on
     * @return Some Object representing a "key" on which to base deduplication
     */
    Object extractKey(T t);

    /**
     * Upon occurrence of duplication, apply a reduction to produce a deduplicated result
     *
     * @return A reduction of two items deemed to be duplicates (have same key)
     */
    T reduceDuplicates(T first, T second);

    class Composed<T> implements Deduplication<T> {

        private final Function<? super T, ?> keyExtractor;

        private final BinaryOperator<T> reducer;

        private Composed(Function<? super T, ?> keyExtractor, BinaryOperator<T> reducer) {
            this.keyExtractor = keyExtractor;
            this.reducer = reducer;
        }

        @Override
        public Object extractKey(T t) {
            return keyExtractor.apply(t);
        }

        @Override
        public T reduceDuplicates(T first, T second) {
            return reducer.apply(first, second);
        }
    }
}
