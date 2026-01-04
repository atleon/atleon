package io.atleon.polling.reactive;

import java.time.Duration;
import java.util.function.Supplier;
import reactor.core.scheduler.Scheduler;

public class PollerOptions {

    private final Duration pollingInterval;
    private final Supplier<Scheduler> schedulerSupplier;

    public PollerOptions(final Duration pollingInterval, final Supplier<Scheduler> schedulerSupplier) {
        this.pollingInterval = pollingInterval;
        this.schedulerSupplier = schedulerSupplier;
    }

    public static PollerOptions create(final Duration pollingInterval, final Supplier<Scheduler> schedulerSupplier) {
        return new PollerOptions(pollingInterval, schedulerSupplier);
    }

    public Supplier<Scheduler> getSchedulerSupplier() {
        return schedulerSupplier;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }
}
