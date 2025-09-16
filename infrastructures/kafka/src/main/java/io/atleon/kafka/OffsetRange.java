package io.atleon.kafka;

/**
 * Describes a range of Offsets where the minimum offset will satisfy the min Criteria and the
 * maximum offset will satisfy the max Criteria.
 */
public final class OffsetRange {

    private final OffsetCriteria minInclusive;

    private final OffsetCriteria maxInclusive;

    private OffsetRange(OffsetCriteria minInclusive, OffsetCriteria maxInclusive) {
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;
    }

    public static OffsetRange of(OffsetCriteria minInclusive, OffsetCriteria maxInclusive) {
        return new OffsetRange(minInclusive, maxInclusive);
    }

    public OffsetCriteria minInclusive() {
        return minInclusive;
    }

    public OffsetCriteria maxInclusive() {
        return maxInclusive;
    }
}
