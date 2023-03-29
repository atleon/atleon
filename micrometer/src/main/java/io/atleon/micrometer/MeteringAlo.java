package io.atleon.micrometer;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;
import io.micrometer.core.instrument.Tags;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Decorates {@link Alo} elements with metering, e.g. start counts, processing duration, etc.
 *
 * @param <T> The type of data item exposed by the decorated {@link Alo}
 */
public class MeteringAlo<T> extends AbstractDecoratingAlo<T> {

    protected final MeterFacade meterFacade;

    protected final Tags baseTags;

    protected final Instant startedAt;

    private MeteringAlo(Alo<T> delegate, MeterFacade meterFacade, Tags baseTags, Instant startedAt) {
        super(delegate);
        this.meterFacade = meterFacade;
        this.baseTags = baseTags;
        this.startedAt = startedAt;
    }

    public static <T> MeteringAlo<T> start(Alo<T> delegate, MeterFacade meterFacade, Tags baseTags) {
        meterFacade.counter("atleon.alo.start", baseTags).increment();
        return new MeteringAlo<>(delegate, meterFacade, baseTags, Instant.now());
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return new MeteringAlo<>(delegate.map(mapper), meterFacade, baseTags, startedAt);
    }

    @Override
    public Runnable getAcknowledger() {
        return applyMetering(delegate.getAcknowledger(), meterFacade, baseTags, startedAt);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return applyMetering(delegate.getNacknowledger(), meterFacade, baseTags, startedAt);
    }

    private static Runnable
    applyMetering(Runnable acknowledger, MeterFacade meterFacade, Tags baseTags, Instant startedAt) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                meterFacade.timer("atleon.alo.duration", baseTags.and("result", "success"))
                    .record(Duration.between(startedAt, Instant.now()));
            }
        };
    }

    private static Consumer<Throwable>
    applyMetering(Consumer<? super Throwable> nacknowledger, MeterFacade meterFacade, Tags baseTags, Instant startedAt) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                meterFacade.timer("atleon.alo.duration", baseTags.and("result", "failure"))
                    .record(Duration.between(startedAt, Instant.now()));
            }
        };
    }
}
