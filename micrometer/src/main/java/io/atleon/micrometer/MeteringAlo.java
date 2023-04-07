package io.atleon.micrometer;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;

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

    protected final MeterKey baseMeterKey;

    protected final Instant startedAt;

    private MeteringAlo(Alo<T> delegate, MeterFacade meterFacade, MeterKey baseMeterKey, Instant startedAt) {
        super(delegate);
        this.meterFacade = meterFacade;
        this.baseMeterKey = baseMeterKey;
        this.startedAt = startedAt;
    }

    public static <T> MeteringAlo<T> start(Alo<T> delegate, MeterFacade meterFacade, MeterKey baseMeterKey) {
        meterFacade.counter(baseMeterKey.withNameQualifier("start")).increment();
        return new MeteringAlo<>(delegate, meterFacade, baseMeterKey, Instant.now());
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return new MeteringAlo<>(delegate.map(mapper), meterFacade, baseMeterKey, startedAt);
    }

    @Override
    public Runnable getAcknowledger() {
        return applyMetering(delegate.getAcknowledger(), meterFacade, baseMeterKey, startedAt);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return applyMetering(delegate.getNacknowledger(), meterFacade, baseMeterKey, startedAt);
    }

    private static Runnable applyMetering(
        Runnable acknowledger,
        MeterFacade meterFacade,
        MeterKey baseMeterKey,
        Instant startedAt
    ) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                meterFacade.timer(baseMeterKey.withNameQualifierAndTag("duration", "result", "success"))
                    .record(Duration.between(startedAt, Instant.now()));
            }
        };
    }

    private static Consumer<Throwable> applyMetering(
        Consumer<? super Throwable> nacknowledger,
        MeterFacade meterFacade,
        MeterKey baseMeterKey,
        Instant startedAt
    ) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                meterFacade.timer(baseMeterKey.withNameQualifierAndTag("duration", "result", "failure"))
                    .record(Duration.between(startedAt, Instant.now()));
            }
        };
    }
}
