package io.atleon.core;

import io.atleon.util.ConfigLoading;
import reactor.core.publisher.Signal;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for loading {@link AloSignalObserver}s
 */
public final class AloSignalObserverConfig {

    /**
     * Optional comma-separated list of {@link AloSignalObserver} types. Each type is either a
     * predefined type defined in this class or a fully qualified name of a class that implements
     * {@link AloSignalObserver}. Defaults to  {@link #SIGNAL_OBSERVER_TYPE_AUTO}, which results in
     * automatic loading of decorators through the {@link java.util.ServiceLoader} SPI.
     */
    public static final String ALO_SIGNAL_OBSERVER_TYPES_CONFIG = "alo.signal.observer.types";

    /**
     * Type name used to activate automatic loading of {@link AloSignalObserver}s through
     * {@link java.util.ServiceLoader ServiceLoader} SPI
     */
    public static final String SIGNAL_OBSERVER_TYPE_AUTO = "auto";

    private AloSignalObserverConfig() {

    }

    public static <T, D extends AloSignalObserver<T>> Optional<AloSignalObserver<T>>
    load(Map<String, ?> properties, Class<D> superType) {
        List<AloSignalObserver<T>> observers = loadExplicit(properties, superType)
            .orElseGet(() -> ConfigLoading.loadListOfConfiguredServices(superType, properties));
        return observers.isEmpty() ? Optional.empty() : Optional.of(AloSignalObserver.combine(observers));
    }

    private static <T, D extends AloSignalObserver<T>> Optional<List<AloSignalObserver<T>>>
    loadExplicit(Map<String, ?> properties, Class<D> superType) {
        return ConfigLoading.loadListOfConfiguredWithPredefinedTypes(
            properties,
            ALO_SIGNAL_OBSERVER_TYPES_CONFIG,
            superType,
            typeName -> instantiatePredefined(properties, superType, typeName)
        );
    }

    private static <T> Optional<AloSignalObserver<T>> instantiatePredefined(
        Map<String, ?> properties,
        Class<? extends AloSignalObserver<T>> superType,
        String typeName
    ) {
        if (typeName.equalsIgnoreCase(SIGNAL_OBSERVER_TYPE_AUTO)) {
            List<AloSignalObserver<T>> observers = ConfigLoading.loadListOfConfiguredServices(superType, properties);
            return Optional.of(observers.isEmpty() ? new NoOp<>() : AloSignalObserver.combine(observers));
        } else {
            return Optional.empty();
        }
    }

    private static final class NoOp<T> implements AloSignalObserver<T> {

        @Override
        public void accept(Signal<Alo<T>> signal) {

        }
    }
}
