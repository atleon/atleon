package io.atleon.util;

/**
 * Utility methods for performing relevant non-trivial mathematical calculations.
 */
public final class Calculation {

    private Calculation() {}

    /**
     * Calculates the binary logarithm (log base 2) of the given value, returning the floor of the
     * result as an integer.
     *
     * @param value the positive integer value to calculate the binary log for
     * @return the floor of log₂(value)
     * @throws IllegalArgumentException if value is less than or equal to 0
     */
    public static int binaryLogOf(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Cannot calculate binary log of non-positive value: " + value);
        }
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * Calculates the power of two that is less than or equal to the provided value.
     *
     * @throws IllegalArgumentException if value is less than or equal to 0
     */
    public static int greatestPowerOfTwoIn(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Cannot calculate highest power of 2 in non-positive value: " + value);
        }
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value ^ (value >> 1);
    }
}
