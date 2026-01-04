package io.atleon.micrometer;

import io.atleon.core.AloQueueListener;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Templated implementation of metering around the queuing of in-flight {@link io.atleon.core.Alo}
 * items
 */
public abstract class MeteringAloQueueListener<K> implements AloQueueListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MeteringAloQueueListener.class);

    private static final double ABSENT_EVALUATION = Double.NaN;

    private static final AuditedMetricRegistry<MeteringAloQueueListener<?>, Object, Double> IN_FLIGHT_GROUP_REGISTRY =
            new AuditedMetricRegistry<>(MeteringAloQueueListener::evaluateInFlight, ABSENT_EVALUATION);

    private final MeterRegistry meterRegistry;

    private final String inFlightMetricName;

    private final Map<K, AtomicLong> inFlightsByGroupKey = new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    protected MeteringAloQueueListener(String inFlightMetricName) {
        this(Metrics.globalRegistry, inFlightMetricName);
    }

    protected MeteringAloQueueListener(MeterRegistry meterRegistry, String inFlightMetricName) {
        this.meterRegistry = meterRegistry;
        this.inFlightMetricName = inFlightMetricName;
    }

    @Override
    public final void created(Object group) {
        synchronized (inFlightsByGroupKey) {
            if (!closed) {
                inFlightsByGroupKey.computeIfAbsent(extractKey(group), this::registerNewGroup);
            }
        }
    }

    @Override
    public final void enqueued(Object group, long count) {
        addToInFlight(extractKey(group), count);
    }

    @Override
    public final void dequeued(Object group, long count) {
        addToInFlight(extractKey(group), -count);
    }

    @Override
    public void close() {
        synchronized (inFlightsByGroupKey) {
            closed = true;
            IN_FLIGHT_GROUP_REGISTRY.unregister(this);
            inFlightsByGroupKey.clear();
        }
    }

    protected abstract K extractKey(Object group);

    protected final AtomicLong registerNewGroup(K groupKey) {
        MeterKey meterKey = new MeterKey(inFlightMetricName, extractTags(groupKey));
        registerGauge(IN_FLIGHT_GROUP_REGISTRY.register(meterKey, this, groupKey));
        return new AtomicLong();
    }

    protected abstract Iterable<Tag> extractTags(K groupKey);

    private void addToInFlight(K groupKey, long count) {
        AtomicLong inFlight = inFlightsByGroupKey.get(groupKey);
        if (inFlight != null) {
            inFlight.addAndGet(count);
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    private double evaluateInFlight(Object groupKey) {
        AtomicLong inFlight = inFlightsByGroupKey.get(groupKey);
        return inFlight == null ? ABSENT_EVALUATION : inFlight.doubleValue();
    }

    private void registerGauge(MeterKey meterKey) {
        try {
            Gauge.builder(meterKey.getName(), meterKey, IN_FLIGHT_GROUP_REGISTRY::evaluate)
                    .description("The number of in-flight Alo items awaiting acknowledgement execution")
                    .tags(meterKey.getTags())
                    .register(meterRegistry);
        } catch (Exception e) {
            LOGGER.debug("Failed to register Gauge with key={}", meterKey, e);
        }
    }
}
