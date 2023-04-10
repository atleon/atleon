package io.atleon.micrometer;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Decorates {@link Alo} elements with metering, e.g. processing duration
 *
 * @param <T> The type of data item exposed by the decorated {@link Alo}
 */
public class MeteringAlo<T> extends AbstractDecoratingAlo<T> {

    protected final AloMeters meters;

    protected final long startedAtNano;

    private MeteringAlo(Alo<T> delegate, AloMeters meters, long startedAtNano) {
        super(delegate);
        this.meters = meters;
        this.startedAtNano = startedAtNano;
    }

    public static <T> MeteringAlo<T> start(Alo<T> delegate, AloMeters meters) {
        return new MeteringAlo<>(delegate, meters, System.nanoTime());
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return new MeteringAlo<>(delegate.map(mapper), meters, startedAtNano);
    }

    @Override
    public Runnable getAcknowledger() {
        return applyMetering(delegate.getAcknowledger(), meters, startedAtNano);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return applyMetering(delegate.getNacknowledger(), meters, startedAtNano);
    }

    private static Runnable applyMetering(Runnable acknowledger, AloMeters meters, long startedAtNano) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                meters.success(Duration.ofNanos(System.nanoTime() - startedAtNano));
            }
        };
    }

    private static Consumer<Throwable>
    applyMetering(Consumer<? super Throwable> nacknowledger, AloMeters meters, long startedAtNano) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                meters.failure(Duration.ofNanos(System.nanoTime() - startedAtNano));
            }
        };
    }
}
