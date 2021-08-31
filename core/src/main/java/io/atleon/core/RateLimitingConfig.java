package io.atleon.core;

/**
 * Configures quantitative behavior of emission rate limiting in reactive pipelines
 */
public final class RateLimitingConfig {

    private final double permitsPerSecond;

    public RateLimitingConfig(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    public boolean isEnabled() {
        return permitsPerSecond > 0D;
    }

    public double getPermitsPerSecond() {
        return permitsPerSecond;
    }
}
