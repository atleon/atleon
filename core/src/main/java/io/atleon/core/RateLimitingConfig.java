package io.atleon.core;

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
