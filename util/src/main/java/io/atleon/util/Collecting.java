package io.atleon.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility methods related to performing relevant non-trivial/performant collection operations.
 */
public final class Collecting {

    private Collecting() {}

    /**
     * Generates binary strided selections of the provided collection. Each selection corresponds
     * to a particular <i>block size</i> and <i>parity</i> (odd or even). <i>Block size</i> starts
     * with 1 and is doubled on every even selection, and the <i>parity</i> dictates which
     * near-half of the collection is selected. For a given even selection, it will contain
     * <i>every other</i> "block" from the source collection. The subsequent odd selection will
     * contain all the blocks not selected in the previous selection. For example, given a list of
     * [0, 1, 2, 3, 4, 5, 6], its binary strided selections would be:
     * <ol>
     *     <li>[0, 2, 4, 6]</li>
     *     <li>[1, 3, 5]</li>
     *     <li>[0, 1, 4, 5]</li>
     *     <li>[2, 3, 6]</li>
     *     <li>[0, 1, 2, 3]</li>
     *     <li>[4, 5, 6]</li>
     * </ol>
     *
     * @param <T>                the type of elements in the collection
     * @param <C>                the type of collection to create for each strided permutation
     * @param collection         the source collection to permute
     * @param collectionSupplier supplier that initializes each permutation collection
     * @return a list of permutations, each containing elements selected via binary stride pattern
     */
    public static <T, C extends Collection<T>> List<C> binaryStrides(
            Collection<T> collection, Supplier<C> collectionSupplier) {
        if (collection.size() <= 1) {
            C result = collectionSupplier.get();
            result.addAll(collection);
            return Collections.singletonList(result);
        } else {
            List<T> list = new ArrayList<>(collection);
            return IntStream.range(0, totalBinaryStrides(list.size()))
                    .mapToObj(it -> binaryStride(list, it, collectionSupplier))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Finds all elements in a collection that have the greatest value when evaluated by the
     * provided function. Returns a collection containing all elements that produce the maximum
     * comparable value.
     *
     * @param <T>                the type of elements in the input collection
     * @param <V>                the type of comparable value produced by the evaluator
     * @param <C>                the type of collection to return
     * @param collection         the source collection to evaluate
     * @param evaluator          function that maps each element to a comparable value
     * @param collectionSupplier supplier that creates the result collection instance
     * @return a collection containing all elements with the greatest evaluated value
     */
    public static <T, V extends Comparable<? super V>, C extends Collection<T>> C greatest(
            Collection<T> collection, Function<? super T, ? extends V> evaluator, Supplier<C> collectionSupplier) {
        C result = collectionSupplier.get();
        V greatestValue = null;
        for (T element : collection) {
            V value = evaluator.apply(element);
            int comparison = greatestValue == null ? 1 : value.compareTo(greatestValue);
            if (comparison > 0) {
                greatestValue = value;
                result.clear();
                result.add(element);
            } else if (comparison == 0) {
                result.add(element);
            }
        }
        return result;
    }

    /**
     * Collects the elements from a provided collection, up-to-and-including the first element that
     * matches the provided predicate, into a new collection as initialized by the provided
     * {@link Supplier}. The "first" element is determined by the encounter order of the source
     * collection.
     *
     * @param <T>                the type of elements in the input collection
     * @param <C>                the type of collection to return
     * @param collection         the source collection to evaluate
     * @param predicate          the predicate on which detect terminating element
     * @param collectionSupplier supplier that creates the result collection instance
     * @return A <i>new</i> collection containing elements up-to-and-including matching element
     */
    public static <T, C extends Collection<T>> C takeUntil(
            Collection<T> collection, Predicate<? super T> predicate, Supplier<C> collectionSupplier) {
        C result = collectionSupplier.get();
        for (T element : collection) {
            result.add(element);
            if (predicate.test(element)) {
                return result;
            }
        }
        return result;
    }

    /**
     * Computes the difference between two collections, returning elements that are present
     * in the left collection but not in the right collection.
     *
     * @param <T>   the type of elements in the collections
     * @param left  the collection from which to subtract elements
     * @param right the collection containing elements to subtract
     * @return a collection containing elements in left but not in right
     */
    public static <T> Collection<T> difference(Collection<T> left, Collection<T> right) {
        return left.isEmpty() || right.isEmpty()
                ? left
                : left.stream().filter(it -> !right.contains(it)).collect(Collectors.toList());
    }

    private static <T, C extends Collection<T>> C binaryStride(List<T> source, int cycle, Supplier<C> resultCreator) {
        C result = resultCreator.get();
        binaryStride(source, cycle, result::add);
        return result;
    }

    private static <T> void binaryStride(List<T> source, int cycle, Consumer<T> accumulator) {
        int blockSize = 1 << (cycle / 2);
        int parity = cycle % 2;

        int stopIndex = Calculation.greatestPowerOfTwoIn(source.size() + 1);
        for (int index = blockSize * parity; index < stopIndex; index += blockSize * 2) {
            int stopBlockIndex = Math.min(blockSize, source.size() - index);
            for (int blockIndex = 0; blockIndex < stopBlockIndex; blockIndex++) {
                accumulator.accept(source.get(index + blockIndex));
            }
        }

        int remaining = source.size() - stopIndex;
        if (remaining > 1) {
            int maxCycle = totalBinaryStrides(remaining) - 2 + parity;
            binaryStride(source.subList(stopIndex, source.size()), Math.min(cycle, maxCycle), accumulator);
        } else if (remaining == 1 && parity == 0) {
            accumulator.accept(source.get(stopIndex));
        }
    }

    private static int totalBinaryStrides(int size) {
        return 2 * Calculation.binaryLogOf(size + 1);
    }
}
