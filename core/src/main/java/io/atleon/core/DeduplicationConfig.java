package io.atleon.core;

import io.atleon.util.Defaults;

import java.time.Duration;

/**
 * Configures quantitative behavior of deduplication
 */
public final class DeduplicationConfig {

    private final Duration deduplicationDuration;

    private final long maxDeduplicationSize;

    private final int deduplicationConcurrency;

    private final int deduplicationSourcePrefetch;

    /**
     * @param deduplicationDuration The Duration in which to deduplicate items
     */
    public DeduplicationConfig(Duration deduplicationDuration) {
        this(deduplicationDuration, Long.MAX_VALUE);
    }

    /**
     * @param deduplicationDuration The max Duration in which to deduplicate items
     * @param maxDeduplicationSize The max number of items with any given key to dedpulicate
     */
    public DeduplicationConfig(Duration deduplicationDuration, long maxDeduplicationSize) {
        this(deduplicationDuration, maxDeduplicationSize, Integer.MAX_VALUE);
    }

    /**
     * @param deduplicationDuration The max Duration in which to deduplicate items
     * @param maxDeduplicationSize The max number of items with any given key to dedpulicate
     * @param deduplicationConcurrency The max number of concurrent deduplications to allow
     */
    public DeduplicationConfig(Duration deduplicationDuration, long maxDeduplicationSize, int deduplicationConcurrency) {
        this(deduplicationDuration, maxDeduplicationSize, deduplicationConcurrency, Defaults.PREFETCH);
    }

    /**
     * @param deduplicationDuration The max Duration in which to deduplicate items
     * @param maxDeduplicationSize The max number of items with any given key to dedpulicate
     * @param deduplicationConcurrency The max number of concurrent deduplications to allow
     * @param deduplicationSourcePrefetch Prefetch on the deduplicated source
     */
    public DeduplicationConfig(
        Duration deduplicationDuration,
        long maxDeduplicationSize,
        int deduplicationConcurrency,
        int deduplicationSourcePrefetch
    ) {
        this.deduplicationDuration = deduplicationDuration;
        this.maxDeduplicationSize = maxDeduplicationSize;
        this.deduplicationConcurrency = deduplicationConcurrency;
        this.deduplicationSourcePrefetch = deduplicationSourcePrefetch;
    }

    public boolean isEnabled() {
        return !deduplicationDuration.isNegative() && !deduplicationDuration.isZero();
    }

    public Duration getDeduplicationDuration() {
        return deduplicationDuration;
    }

    public long getMaxDeduplicationSize() {
        return maxDeduplicationSize;
    }

    public int getDeduplicationConcurrency() {
        return deduplicationConcurrency;
    }

    public int getDeduplicationSourcePrefetch() {
        return deduplicationSourcePrefetch;
    }
}
