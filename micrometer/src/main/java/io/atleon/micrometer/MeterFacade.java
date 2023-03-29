package io.atleon.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Facade wrapping a {@link MeterRegistry} with convenience methods for creating/managing Meters.
 */
public final class MeterFacade {

    private final MeterRegistry registry;

    private final Map<MeterKey, Counter> counters = new ConcurrentHashMap<>();

    private final Map<MeterKey, Timer> timers = new ConcurrentHashMap<>();

    private MeterFacade(MeterRegistry registry) {
        this.registry = registry;
    }

    public static MeterFacade global() {
        return new MeterFacade(Metrics.globalRegistry);
    }

    public static MeterFacade wrap(MeterRegistry registry) {
        return new MeterFacade(registry);
    }

    public Counter counter(String name, Tags tags) {
        return counters.computeIfAbsent(new MeterKey(name, tags), this::newCounter);
    }

    public Timer timer(String name, Tags tags) {
        return timers.computeIfAbsent(new MeterKey(name, tags), this::newTimer);
    }

    private Counter newCounter(MeterKey meterKey) {
        return registry.counter(meterKey.getName(), meterKey.getTags());
    }

    private Timer newTimer(MeterKey meterKey) {
        return registry.timer(meterKey.getName(), meterKey.getTags());
    }
}
