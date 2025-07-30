package io.atleon.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CalculationTest {

    @Test
    public void binaryLogOf_givenValues_expectsCorrectCalculation() {
        assertThrows(IllegalArgumentException.class, () -> Calculation.binaryLogOf(-1));
        assertThrows(IllegalArgumentException.class, () -> Calculation.binaryLogOf(0));
        assertEquals(0, Calculation.binaryLogOf(1));
        assertEquals(1, Calculation.binaryLogOf(2));
        assertEquals(1, Calculation.binaryLogOf(3));
        assertEquals(2, Calculation.binaryLogOf(4));
        assertEquals(2, Calculation.binaryLogOf(5));
        assertEquals(2, Calculation.binaryLogOf(6));
        assertEquals(2, Calculation.binaryLogOf(7));
        assertEquals(3, Calculation.binaryLogOf(8));
    }

    @Test
    public void greatestPowerOfTwoIn_givenValues_expectsPowerOfTwoLessThanOrGreaterToValue() {
        assertThrows(IllegalArgumentException.class, () -> Calculation.greatestPowerOfTwoIn(-1));
        assertThrows(IllegalArgumentException.class, () -> Calculation.greatestPowerOfTwoIn(0));
        assertEquals(1, Calculation.greatestPowerOfTwoIn(1));
        assertEquals(2, Calculation.greatestPowerOfTwoIn(2));
        assertEquals(2, Calculation.greatestPowerOfTwoIn(3));
        assertEquals(4, Calculation.greatestPowerOfTwoIn(4));
        assertEquals(4, Calculation.greatestPowerOfTwoIn(7));
        assertEquals(8, Calculation.greatestPowerOfTwoIn(8));
    }
}