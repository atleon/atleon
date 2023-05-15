package io.atleon.micrometer;

import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Encapsulation of {@link io.micrometer.core.instrument.Meter}s used when applying metering to
 * {@link io.atleon.core.Alo}
 */
public final class AloMeters {

    private final Timer successTimer;

    private final Timer failureTimer;

    AloMeters(Timer successTimer, Timer failureTimer) {
        this.successTimer = successTimer;
        this.failureTimer = failureTimer;
    }

    public void success(Duration duration) {
        successTimer.record(duration);
    }

    public void failure(Duration duration) {
        failureTimer.record(duration);
    }
}
