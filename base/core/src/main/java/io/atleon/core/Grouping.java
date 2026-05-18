package io.atleon.core;

import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Specifies how to group elements from some source emitting elements of type T into groups keyed
 * by type K.
 *
 * @param <T> The type of elements on which grouping is applied
 * @param <K> The type of key indicating group membership
 */
interface Grouping<T, K> {

    static <T, K> Grouping<T, K> simple(Function<? super T, ? extends K> keyExtractor) {
        return new Composed<>(keyExtractor, __ -> false, null);
    }

    /**
     * Extracts a group-identifying key for the provided element for emission.
     */
    K extractKey(T element);

    /**
     * Indicates whether the provided element should be the completing element for the group to
     * which it belongs.
     */
    boolean completesGroup(T element);

    /**
     * Specifies the number of elements to request from the source for grouping. An empty value
     * indicates usage of operator-specific default. A present value of Integer.MAX_VALUE indicates
     * unbounded request.
     */
    Optional<Integer> sourcePrefetch();

    final class Composed<T, K> implements Grouping<T, K> {

        private final Function<? super T, ? extends K> keyExtractor;

        private final Predicate<? super T> completesGroup;

        private final @Nullable Integer sourcePrefetch;

        private Composed(
                Function<? super T, ? extends K> keyExtractor,
                Predicate<? super T> completesGroup,
                @Nullable Integer sourcePrefetch) {
            this.keyExtractor = keyExtractor;
            this.completesGroup = completesGroup;
            this.sourcePrefetch = sourcePrefetch;
        }

        @Override
        public K extractKey(T element) {
            return keyExtractor.apply(element);
        }

        @Override
        public boolean completesGroup(T element) {
            return completesGroup.test(element);
        }

        @Override
        public Optional<Integer> sourcePrefetch() {
            return Optional.ofNullable(sourcePrefetch);
        }
    }
}
