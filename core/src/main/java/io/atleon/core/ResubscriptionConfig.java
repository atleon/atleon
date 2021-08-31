package io.atleon.core;

import java.time.Duration;

/**
 * Configures quantitative behavior of resubscription
 */
public final class ResubscriptionConfig {

    public static final Duration DEFAULT_DELAY = Duration.ofSeconds(20L);

    private final String name;

    private final Duration delay;

    /**
     * @param name Name used for logging purposes to indicate error occurrences
     */
    public ResubscriptionConfig(String name) {
        this(name, DEFAULT_DELAY);
    }

    /**
     * @param name Name used for logging purposes to indicate error occurrences
     * @param delay The delay between occurrence of an error and resubscribing
     */
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
