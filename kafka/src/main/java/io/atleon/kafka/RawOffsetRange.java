package io.atleon.kafka;

/**
 * Describes a "raw" range of offsets.
 */
public final class RawOffsetRange {

    private final long minInclusive;

    private final long maxInclusive;

    private RawOffsetRange(long minInclusive, long maxInclusive) {
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;
    }

    public static RawOffsetRange of(long minInclusive, long maxInclusive) {
        return new RawOffsetRange(minInclusive, maxInclusive);
    }

    public OffsetRange toOffsetRange() {
        return OffsetCriteria.raw(minInclusive).to(OffsetCriteria.raw(maxInclusive));
    }
}
