package io.atleon.core;

import io.atleon.util.Configurable;
import reactor.core.observability.SignalListenerFactory;

import java.util.Map;

/**
 * Interface for creating {@link reactor.core.observability.SignalListener} instances that add
 * side-effects to invocation of reactive methods in pipelines of {@link Alo} items.
 * Implementations of this interface can be passed to {@link AloFlux#tap(SignalListenerFactory)}.
 *
 * @param <T> The type of data exposed by Alo values in emitted onNext signals
 */
public interface AloSignalListenerFactory<T, STATE> extends SignalListenerFactory<Alo<T>, STATE>, Configurable {

    @Override
    default void configure(Map<String, ?> properties) {}
}
