package io.atleon.core;

import io.atleon.util.Defaults;

import java.time.Duration;

public final class DeduplicationConfig {

    private final Duration deduplicationDuration;

    private final long maxDeduplicationSize;

    private final int deduplicationConcurrency;

    private final int deduplicationSourcePrefetch;

    public DeduplicationConfig(Duration deduplicationDuration) {
        this(deduplicationDuration, Long.MAX_VALUE, Defaults.CONCURRENCY, Defaults.PREFETCH);
    }

    public DeduplicationConfig(Duration deduplicationDuration, long maxDeduplicationSize) {
        this(deduplicationDuration, maxDeduplicationSize, Defaults.CONCURRENCY, Defaults.PREFETCH);
    }

    public DeduplicationConfig(
        Duration deduplicationDuration,
        long maxDeduplicationSize,
        int deduplicationConcurrency) {
        this(deduplicationDuration, maxDeduplicationSize, deduplicationConcurrency, Defaults.PREFETCH);
    }

    public DeduplicationConfig(
        Duration deduplicationDuration,
        long maxDeduplicationSize,
        int deduplicationConcurrency,
        int deduplicationSourcePrefetch) {
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
