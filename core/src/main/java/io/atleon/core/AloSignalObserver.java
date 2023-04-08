package io.atleon.core;

import io.atleon.util.Configurable;
import reactor.core.publisher.Signal;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Interface for adding side-effects to the transmission of {@link Signal}s in Reactor
 * pipelines of {@link Alo} items. This implementations of this interface can be used in
 * {@code doOnEach} operators of Alo Publisher pipelines (like {@link AloFlux#doOnEachAlo(Consumer)}}
 *
 * @param <T> The type of data exposed by Alo values in emitted Signals
 */
public interface AloSignalObserver<T> extends Configurable, Consumer<Signal<Alo<T>>> {

    static <T> AloSignalObserver<T> combine(List<AloSignalObserver<T>> observers) {
        return observers.size() == 1 ? observers.get(0) : new Composite<>(observers);
    }

    @Override
    default void configure(Map<String, ?> properties) {

    }

    class Composite<T> implements AloSignalObserver<T> {

        private final List<AloSignalObserver<T>> observers;

        private Composite(List<AloSignalObserver<T>> observers) {
            this.observers = observers;
        }

        @Override
        public void configure(Map<String, ?> properties) {
            observers.forEach(observer -> observer.configure(properties));
        }

        @Override
        public void accept(Signal<Alo<T>> signal) {
            observers.forEach(observer -> observer.accept(signal));
        }
    }
}
