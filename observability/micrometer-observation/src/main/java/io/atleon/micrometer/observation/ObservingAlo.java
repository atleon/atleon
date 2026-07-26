package io.atleon.micrometer.observation;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Decorates {@link Alo} elements with observation
 *
 * @param <T> The type of data item exposed by the decorated {@link Alo}
 */
public class ObservingAlo<T> extends AbstractDecoratingAlo<T> {

    protected final ObservationRegistry registry;

    protected final Observation observation;

    private ObservingAlo(Alo<T> delegate, ObservationRegistry registry, Observation observation) {
        super(delegate);
        this.registry = registry;
        this.observation = observation;
    }

    public static <T> ObservingAlo<T> start(Alo<T> delegate, ObservationRegistry registry, Observation observation) {
        return new ObservingAlo<>(delegate, registry, observation.start());
    }

    @Override
    public void runInContext(Runnable runnable) {
        try (Observation.Scope __ = observation.openScope()) {
            delegate.runInContext(runnable);
        }
    }

    @Override
    public <R> R supplyInContext(Supplier<R> supplier) {
        try (Observation.Scope __ = observation.openScope()) {
            return delegate.supplyInContext(supplier);
        }
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        try (Observation.Scope __ = observation.openScope()) {
            return new ObservingAlo<>(delegate.map(mapper), registry, observation);
        }
    }

    @Override
    public <R> AloFactory<List<R>> fanInPropagator(List<? extends Alo<?>> alos) {
        return delegate.<R>fanInPropagator(alos).withDecorator(alo -> {
            Observation fanInObservation = Observation.createNotStarted("atleon.fan.in", registry);
            // TODO Should link contexts from fanned-in Alos. Micrometer does not (yet) support
            //      this, so best we can do is use this instance's Observation as a "parent".
            //      https://github.com/micrometer-metrics/tracing/issues/1160
            return start(alo, registry, fanInObservation.parentObservation(observation));
        });
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return delegate.<R>propagator().withDecorator(alo -> new Propagated<>(alo, registry, observation));
    }

    @Override
    public Runnable getAcknowledger() {
        return applyObservationTermination(delegate.getAcknowledger(), observation);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return applyObservationTermination(delegate.getNacknowledger(), observation);
    }

    private static Runnable applyObservationTermination(Runnable acknowledger, Observation observation) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                observation.stop();
            }
        };
    }

    private static Consumer<Throwable> applyObservationTermination(
            Consumer<? super Throwable> nacknowledger, Observation observation) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                observation.error(error);
                observation.stop();
            }
        };
    }

    /**
     * This extension is necessary to make one-to-many mappings work correctly. The underlying
     * observation is propagated to all downstream mappings such that it can be activated in
     * context. However, none of those mappings should finish the observation. This is in contrast
     * to "metering" (exclusively) where we do not propagate the meters at all, and simply rely on
     * the originating element's acknowledgement to eventually be executed.
     */
    private static final class Propagated<T> extends ObservingAlo<T> {

        public Propagated(Alo<T> delegate, ObservationRegistry registry, Observation observation) {
            super(delegate, registry, observation);
        }

        @Override
        public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
            try (Observation.Scope __ = observation.openScope()) {
                return new Propagated<>(delegate.map(mapper), registry, observation);
            }
        }

        @Override
        public Runnable getAcknowledger() {
            return delegate.getAcknowledger();
        }

        @Override
        public Consumer<? super Throwable> getNacknowledger() {
            return delegate.getNacknowledger();
        }
    }
}
