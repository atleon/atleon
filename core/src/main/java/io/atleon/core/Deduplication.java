package io.atleon.core;

/**
 * Configures how to deduplicate data items.
 *
 * @param <T> The type of data item being deduplicated
 */
@FunctionalInterface
public interface Deduplication<T> {

    static <T> Deduplication<T> identity() {
        return new Identity<>();
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
    default T reduceDuplicates(T t1, T t2) {
        return t1;
    }

    class Identity<T> implements Deduplication<T> {

        @Override
        public Object extractKey(T t) {
            return t;
        }
    }
}
