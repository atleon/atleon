package io.atleon.core;

import io.atleon.util.ConfigLoading;
import reactor.core.publisher.Signal;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for loading {@link AloSignalListener}s
 */
public final class AloSignalListenerConfig {

    /**
     * Optional comma-separated list of {@link AloSignalListener} types. Each type is either a
     * predefined type defined in this class or a fully qualified name of a class that implements
     * {@link AloSignalListener}. Defaults to  {@link #SIGNAL_LISTENER_TYPE_AUTO}, which results in
     * automatic loading of decorators through the {@link java.util.ServiceLoader} SPI.
     */
    public static final String ALO_SIGNAL_LISTENER_TYPES_CONFIG = "alo.signal.listener.types";

    /**
     * Type name used to activate automatic loading of {@link AloSignalListener}s through
     * {@link java.util.ServiceLoader ServiceLoader} SPI
     */
    public static final String SIGNAL_LISTENER_TYPE_AUTO = "auto";

    private AloSignalListenerConfig() {

    }

    public static <T, D extends AloSignalListener<T>> Optional<AloSignalListener<T>>
    load(Map<String, ?> properties, Class<D> superType) {
        List<AloSignalListener<T>> decorators = loadExplicit(properties, superType)
            .orElseGet(() -> ConfigLoading.loadListOfConfiguredServices(superType, properties));
        return decorators.isEmpty() ? Optional.empty() : Optional.of(AloSignalListener.combine(decorators));
    }

    private static <T, D extends AloSignalListener<T>> Optional<List<AloSignalListener<T>>>
    loadExplicit(Map<String, ?> properties, Class<D> superType) {
        return ConfigLoading.loadListOfConfiguredWithPredefinedTypes(
            properties,
            ALO_SIGNAL_LISTENER_TYPES_CONFIG,
            superType,
            typeName -> instantiatePredefined(properties, superType, typeName)
        );
    }

    private static <T> Optional<AloSignalListener<T>> instantiatePredefined(
        Map<String, ?> properties,
        Class<? extends AloSignalListener<T>> superType,
        String typeName
    ) {
        if (typeName.equalsIgnoreCase(SIGNAL_LISTENER_TYPE_AUTO)) {
            List<AloSignalListener<T>> listeners = ConfigLoading.loadListOfConfiguredServices(superType, properties);
            return Optional.of(listeners.isEmpty() ? new NoOp<>() : AloSignalListener.combine(listeners));
        } else {
            return Optional.empty();
        }
    }

    private static final class NoOp<T> implements AloSignalListener<T> {

        @Override
        public void accept(Signal<Alo<T>> signal) {

        }
    }
}
