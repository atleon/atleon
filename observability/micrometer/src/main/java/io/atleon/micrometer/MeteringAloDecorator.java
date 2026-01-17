package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.core.AloDecorator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Templated implementation of {@link Alo} metering decoration
 *
 * @param <T> The type of data referenced by decorated {@link Alo}s
 */
public abstract class MeteringAloDecorator<T, K> implements AloDecorator<T> {

    private enum AloMeterType {
        SUCCESS_TIMER,
        FAILURE_TIMER
    }

    private final MeterFacade<TypeKey<AloMeterType, K>> meterFacade;

    protected MeteringAloDecorator(String name) {
        this(Metrics.globalRegistry, name);
    }

    protected MeteringAloDecorator(MeterRegistry meterRegistry, String name) {
        this.meterFacade = MeterFacade.create(meterRegistry, it -> toMeterKey(name, it));
    }

    @Override
    public void configure(Map<String, ?> properties) {
        meterFacade.clear();
    }

    @Override
    public int order() {
        return INNERMOST_ORDER + 4000;
    }

    @Override
    public final Alo<T> decorate(Alo<T> alo) {
        T data = alo.get();
        K key = extractKey(data);

        Timer successTimer = meterFacade.timer(new TypeKey<>(AloMeterType.SUCCESS_TIMER, key));
        Timer failureTimer = meterFacade.timer(new TypeKey<>(AloMeterType.FAILURE_TIMER, key));

        long startedAtNano = System.nanoTime();
        Runnable acknowledger = applyMetering(alo.getAcknowledger(), successTimer, startedAtNano);
        Consumer<Throwable> nacknowledger = applyMetering(alo.getNacknowledger(), failureTimer, startedAtNano);
        return alo.<T>propagator().create(data, acknowledger, nacknowledger);
    }

    /**
     * Extracts a key that is used to identify {@link io.micrometer.core.instrument.Meter}s used to
     * decorate {@link Alo} processing. The cardinality of the key directly correlates with the
     * cardinality of created Meters.
     *
     * @param t Instance of data referenced by decorated {@link Alo} element
     * @return A key used as part of the identifier for created metrics
     */
    protected abstract K extractKey(T t);

    protected final MeterKey toMeterKey(String name, TypeKey<AloMeterType, K> typeKey) {
        switch (typeKey.type()) {
            case SUCCESS_TIMER:
                return new MeterKey(
                        name + ".duration", Tags.of("result", "success").and(extractTags(typeKey.key())));
            case FAILURE_TIMER:
                return new MeterKey(
                        name + ".duration", Tags.of("result", "failure").and(extractTags(typeKey.key())));
            default:
                throw new IllegalStateException("Unimplemented aloMeterType=" + typeKey.type());
        }
    }

    /**
     * Extract base set of {@link io.micrometer.core.instrument.Tag}s for metrics
     */
    protected abstract Iterable<Tag> extractTags(K key);

    private static Runnable applyMetering(Runnable acknowledger, Timer timer, long startedAtNano) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                timer.record(System.nanoTime() - startedAtNano, TimeUnit.NANOSECONDS);
            }
        };
    }

    private static Consumer<Throwable> applyMetering(
            Consumer<? super Throwable> nacknowledger, Timer timer, long startedAtNano) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                timer.record(System.nanoTime() - startedAtNano, TimeUnit.NANOSECONDS);
            }
        };
    }
}
