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
public interface AloSignalListener<T> extends Configurable, Consumer<Signal<Alo<T>>> {

    static <T> AloSignalListener<T> combine(List<AloSignalListener<T>> decorators) {
        return decorators.size() == 1 ? decorators.get(0) : new Composite<>(decorators);
    }

    @Override
    default void configure(Map<String, ?> properties) {

    }

    class Composite<T> implements AloSignalListener<T> {

        private final List<AloSignalListener<T>> decorators;

        private Composite(List<AloSignalListener<T>> decorators) {
            this.decorators = decorators;
        }

        @Override
        public void configure(Map<String, ?> properties) {
            decorators.forEach(decorator -> decorator.configure(properties));
        }

        @Override
        public void accept(Signal<Alo<T>> signal) {
            decorators.forEach(decorator -> decorator.accept(signal));
        }
    }
}
