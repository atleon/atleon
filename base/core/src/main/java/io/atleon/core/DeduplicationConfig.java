package io.atleon.core;

import io.atleon.util.Defaults;

import java.time.Duration;

/**
 * Configures quantitative behavior of deduplication
 */
public final class DeduplicationConfig {

    private final Duration deduplicationTimeout;

    private final long maxDeduplicationSize;

    private final int deduplicationConcurrency;

    private final int deduplicationSourcePrefetch;

    /**
     * @param deduplicationTimeout The Duration in which to deduplicate items
     */
    public DeduplicationConfig(Duration deduplicationTimeout) {
        this(deduplicationTimeout, Long.MAX_VALUE);
    }

    /**
     * @param deduplicationTimeout The max Duration in which to deduplicate items
     * @param maxDeduplicationSize The max number of items with any given key to dedpulicate
     */
    public DeduplicationConfig(Duration deduplicationTimeout, long maxDeduplicationSize) {
        this(deduplicationTimeout, maxDeduplicationSize, Integer.MAX_VALUE);
    }

    /**
     * @param deduplicationTimeout The max Duration in which to deduplicate items
     * @param maxDeduplicationSize The max number of items with any given key to dedpulicate
     * @param deduplicationConcurrency The max number of concurrent deduplications to allow
     */
    public DeduplicationConfig(Duration deduplicationTimeout, long maxDeduplicationSize, int deduplicationConcurrency) {
        this(deduplicationTimeout, maxDeduplicationSize, deduplicationConcurrency, Defaults.PREFETCH);
    }

    /**
     * @param deduplicationTimeout The max Duration in which to deduplicate items
     * @param maxDeduplicationSize The max number of items with any given key to dedpulicate
     * @param deduplicationConcurrency The max number of concurrent deduplications to allow
     * @param deduplicationSourcePrefetch Prefetch on the deduplicated source
     */
    public DeduplicationConfig(
            Duration deduplicationTimeout,
            long maxDeduplicationSize,
            int deduplicationConcurrency,
            int deduplicationSourcePrefetch) {
        this.deduplicationTimeout = deduplicationTimeout;
        this.maxDeduplicationSize = maxDeduplicationSize;
        this.deduplicationConcurrency = deduplicationConcurrency;
        this.deduplicationSourcePrefetch = deduplicationSourcePrefetch;
    }

    public boolean isEnabled() {
        return !deduplicationTimeout.isNegative() && !deduplicationTimeout.isZero();
    }

    /**
     * @deprecated Use {@link #getDeduplicationTimeout()}
     */
    @Deprecated
    public Duration getDeduplicationDuration() {
        return getDeduplicationTimeout();
    }

    public Duration getDeduplicationTimeout() {
        return deduplicationTimeout;
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
