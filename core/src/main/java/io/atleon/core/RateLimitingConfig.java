package io.atleon.core;

/**
 * Configures quantitative behavior of emission rate limiting in reactive pipelines
 */
public final class RateLimitingConfig {

    private final double permitsPerSecond;

    private final int prefetch;

    public RateLimitingConfig(double permitsPerSecond, int prefetch) {
        this.permitsPerSecond = permitsPerSecond;
        this.prefetch = prefetch;
    }

    public boolean isEnabled() {
        return permitsPerSecond > 0D;
    }

    public double getPermitsPerSecond() {
        return permitsPerSecond;
    }

    public int getPrefetch() {
        return prefetch;
    }
}
