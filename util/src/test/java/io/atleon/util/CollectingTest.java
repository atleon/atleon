package io.atleon.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CollectingTest {

    @Test
    public void binaryStrides_givenTrivialElements_expectsSingletonSelections() {
        assertEquals(
            Collections.singletonList(Collections.emptyList()),
            Collecting.binaryStrides(Collections.emptyList(), ArrayList::new));
        assertEquals(
            Collections.singletonList(Collections.singletonList(0)),
            Collecting.binaryStrides(Collections.singletonList(0), ArrayList::new));
    }

    @Test
    public void binaryStrides_givenPowerOfTwoElements_expectsBalancedSelections() {
        List<Integer> collection = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);

        List<List<Integer>> result = Collecting.binaryStrides(collection, ArrayList::new);

        assertEquals(6, result.size());
        assertEquals(Arrays.asList(0, 2, 4, 6), result.get(0));
        assertEquals(Arrays.asList(1, 3, 5, 7), result.get(1));
        assertEquals(Arrays.asList(0, 1, 4, 5), result.get(2));
        assertEquals(Arrays.asList(2, 3, 6, 7), result.get(3));
        assertEquals(Arrays.asList(0, 1, 2, 3), result.get(4));
        assertEquals(Arrays.asList(4, 5, 6, 7), result.get(5));
    }

    @Test
    public void binaryStrides_givenPowerOfTwoMinusOneElements_expectsNearBalancedSelections() {
        List<Integer> collection = Arrays.asList(0, 1, 2, 3, 4, 5, 6);

        List<List<Integer>> result = Collecting.binaryStrides(collection, ArrayList::new);

        assertEquals(4, result.size());
        assertEquals(Arrays.asList(0, 2, 4, 6), result.get(0));
        assertEquals(Arrays.asList(1, 3, 5), result.get(1));
        assertEquals(Arrays.asList(0, 1, 4, 5), result.get(2));
        assertEquals(Arrays.asList(2, 3, 6), result.get(3));
    }

    @Test
    public void binaryStrides_givenPowerOfTwoPlusOneElements_expectsNearBalancedSelections() {
        List<Integer> collection = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

        List<List<Integer>> result = Collecting.binaryStrides(collection, ArrayList::new);

        assertEquals(8, result.size());
        assertEquals(Arrays.asList(0, 2, 4, 6, 8, 10, 12, 14, 16), result.get(0));
        assertEquals(Arrays.asList(1, 3, 5, 7, 9, 11, 13, 15), result.get(1));
        assertEquals(Arrays.asList(0, 1, 4, 5, 8, 9, 12, 13, 16), result.get(2));
        assertEquals(Arrays.asList(2, 3, 6, 7, 10, 11, 14, 15), result.get(3));
        assertEquals(Arrays.asList(0, 1, 2, 3, 8, 9, 10, 11, 16), result.get(4));
        assertEquals(Arrays.asList(4, 5, 6, 7, 12, 13, 14, 15), result.get(5));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 16), result.get(6));
        assertEquals(Arrays.asList(8, 9, 10, 11, 12, 13, 14, 15), result.get(7));
    }

    @Test
    public void binaryStrides_givenPowerOfTwoPlusOtherPowerOfTwoElements_expectsNearBalancedSelections() {
        List<Integer> collection = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<List<Integer>> result = Collecting.binaryStrides(collection, ArrayList::new);

        assertEquals(6, result.size());
        assertEquals(Arrays.asList(0, 2, 4, 6, 8), result.get(0));
        assertEquals(Arrays.asList(1, 3, 5, 7, 9), result.get(1));
        assertEquals(Arrays.asList(0, 1, 4, 5, 8), result.get(2));
        assertEquals(Arrays.asList(2, 3, 6, 7, 9), result.get(3));
        assertEquals(Arrays.asList(0, 1, 2, 3, 8), result.get(4));
        assertEquals(Arrays.asList(4, 5, 6, 7, 9), result.get(5));
    }

    @Test
    public void binaryStrides_givenPowerOfTwoPlusMersennePrimeElements_expectsNearBalancedSelections() {
        List<Integer> collection = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<List<Integer>> result = Collecting.binaryStrides(collection, ArrayList::new);

        assertEquals(6, result.size());
        assertEquals(Arrays.asList(0, 2, 4, 6, 8, 10), result.get(0));
        assertEquals(Arrays.asList(1, 3, 5, 7, 9), result.get(1));
        assertEquals(Arrays.asList(0, 1, 4, 5, 8, 10), result.get(2));
        assertEquals(Arrays.asList(2, 3, 6, 7, 9), result.get(3));
        assertEquals(Arrays.asList(0, 1, 2, 3, 8, 10), result.get(4));
        assertEquals(Arrays.asList(4, 5, 6, 7, 9), result.get(5));
    }

    @Test
    public void binaryStrides_givenPowerOfTwoPlusNonMersennePrimeElements_expectsNearBalancedSelections() {
        List<Integer> collection = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        List<List<Integer>> result = Collecting.binaryStrides(collection, ArrayList::new);

        assertEquals(6, result.size());
        assertEquals(Arrays.asList(0, 2, 4, 6, 8, 10, 12), result.get(0));
        assertEquals(Arrays.asList(1, 3, 5, 7, 9, 11), result.get(1));
        assertEquals(Arrays.asList(0, 1, 4, 5, 8, 9, 12), result.get(2));
        assertEquals(Arrays.asList(2, 3, 6, 7, 10, 11), result.get(3));
        assertEquals(Arrays.asList(0, 1, 2, 3, 8, 9, 12), result.get(4));
        assertEquals(Arrays.asList(4, 5, 6, 7, 10, 11), result.get(5));
    }

    @Test
    public void greatest_givenCollectionOfEvaluableElements_expectsElementsWithGreatestEvaluations() {
        List<String> strings = Arrays.asList("lorem", "ipsum", "sit", "adipiscing", "elit", "incididunt", "labore");

        List<String> result = Collecting.greatest(strings, String::length, ArrayList::new);

        assertEquals(Arrays.asList("adipiscing", "incididunt"), result);
    }

    @Test
    public void difference_givenCollections_expectsSubtractedResult() {
        assertEquals(Collections.emptyList(), Collecting.difference(Collections.emptyList(), Collections.emptyList()));
        assertEquals(Arrays.asList(1, 2, 3), Collecting.difference(Arrays.asList(1, 2, 3), Collections.emptyList()));
        assertEquals(Collections.emptyList(), Collecting.difference(Collections.emptyList(), Arrays.asList(4, 5, 6)));
        assertEquals(Arrays.asList(1, 2, 3), Collecting.difference(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6)));
        assertEquals(Arrays.asList(1, 2), Collecting.difference(Arrays.asList(1, 2, 3), Arrays.asList(3, 4, 5)));
    }
}