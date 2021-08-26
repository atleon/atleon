package io.atleon.core;

import java.time.Duration;

public final class ResubscriptionConfig {

    public static final Duration DEFAULT_DELAY = Duration.ofSeconds(20L);

    private final String name;

    private final Duration delay;

    public ResubscriptionConfig(String name) {
        this(name, DEFAULT_DELAY);
    }

    public ResubscriptionConfig(String name, Duration delay) {
        this.name = name;
        this.delay = delay;
    }

    public boolean isEnabled() {
        return !delay.isNegative();
    }

    public String getName() {
        return name;
    }

    public Duration getDelay() {
        return delay;
    }
}
