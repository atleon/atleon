package io.atleon.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Facade wrapping a {@link MeterRegistry} that allows for efficient arbitrary key-based management
 * of {@link io.micrometer.core.instrument.Meter}s. This provides a benefit of accessing Meters
 * with simpler (and therefore, more efficient) types than that of tuple of a name and collection
 * of tags, since the key, when combined with the using context, can typically be modeled as a
 * subset of the information contained in a Meter-identifying tuple.
 */
public final class MeterFacade<K> {

    private final MeterRegistry registry;

    private final Function<? super K, MeterKey> keyToMeterKey;

    private final Map<K, Counter> counters = new ConcurrentHashMap<>();

    private final Map<K, Timer> timers = new ConcurrentHashMap<>();

    private MeterFacade(MeterRegistry registry, Function<? super K, MeterKey> keyToMeterKey) {
        this.registry = registry;
        this.keyToMeterKey = keyToMeterKey;
    }

    public static <K> MeterFacade<K> create(MeterRegistry registry, Function<? super K, MeterKey> keyToMeterKey) {
        return new MeterFacade<>(registry, keyToMeterKey);
    }

    public Counter counter(K key) {
        return counters.computeIfAbsent(key, this::newCounter);
    }

    public Timer timer(K key) {
        return timers.computeIfAbsent(key, this::newTimer);
    }

    public void clear() {
        counters.clear();
        timers.clear();
    }

    private Counter newCounter(K key) {
        MeterKey meterKey = keyToMeterKey.apply(key);
        return registry.counter(meterKey.getName(), meterKey.getTags());
    }

    private Timer newTimer(K key) {
        MeterKey meterKey = keyToMeterKey.apply(key);
        return registry.timer(meterKey.getName(), meterKey.getTags());
    }
}
