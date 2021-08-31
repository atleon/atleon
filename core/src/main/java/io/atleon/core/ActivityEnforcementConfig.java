package io.atleon.core;

import java.time.Duration;

/**
 * Used to configure the enforcement of "activity" on a Publisher. Similar to Reactor's
 * {@link reactor.core.publisher.Flux#timeout(Duration) Flux::timeout}, activity enforcement causes
 * an error to be emitted when a stream is inactive for longer than a specified duration. In
 * contrast to timeout, Activity Enforcement allows specifying a grace period (delay) and also
 * detects timeouts on subscription.
 */
public final class ActivityEnforcementConfig {

    public static final Duration DEFAULT_DELAY = Duration.ofMinutes(1L);

    public static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(1L);

    private final String name;

    private final Duration maxInactivity;

    private final Duration delay;

    private final Duration interval;

    /**
     * @param name A name that can be used to identify on what activity is being enforced
     * @param maxInactivity Max Duration from last signal before considering a stream inactive
     */
    public ActivityEnforcementConfig(String name, Duration maxInactivity) {
        this(name, maxInactivity, DEFAULT_DELAY, DEFAULT_INTERVAL);
    }

    /**
     * @param name A name that can be used to identify on what activity is being enforced
     * @param maxInactivity Max Duration from last signal before considering a stream inactive
     * @param delay Initial delay before checking for inactivity
     */
    public ActivityEnforcementConfig(String name, Duration maxInactivity, Duration delay) {
        this(name, maxInactivity, delay, DEFAULT_INTERVAL);
    }

    /**
     * @param name A name that can be used to identify on what activity is being enforced
     * @param maxInactivity Max Duration from last signal before considering a stream inactive
     * @param delay Initial delay before checking for inactivity
     * @param interval Interval at which to check for an inactive stream
     */
    public ActivityEnforcementConfig(String name, Duration maxInactivity, Duration delay, Duration interval) {
        this.name = name;
        this.maxInactivity = maxInactivity;
        this.delay = delay;
        this.interval = interval;
    }

    public boolean isEnabled() {
        return !maxInactivity.isZero() && !maxInactivity.isNegative();
    }

    public String getName() {
        return name;
    }

    public Duration getMaxInactivity() {
        return maxInactivity;
    }

    public Duration getDelay() {
        return delay;
    }

    public Duration getInterval() {
        return interval;
    }
}
