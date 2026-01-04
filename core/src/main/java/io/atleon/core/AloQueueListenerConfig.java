package io.atleon.core;

import io.atleon.util.ConfigLoading;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for loading {@link AloQueueListener} implementations
 */
public final class AloQueueListenerConfig {

    /**
     * Optional comma-separated list of {@link AloQueueListener} types. Each type is either a
     * predefined type defined in this class or a fully qualified name of a class that implements
     * {@link AloQueueListener}. Defaults to {@link #LISTENER_TYPE_AUTO}, which results in
     * automatic loading of listeners through the {@link java.util.ServiceLoader} SPI.
     */
    public static final String LISTENER_TYPES_CONFIG = "alo.queue.listener.types";

    /**
     * Type name used to activate automatic loading of {@link AloQueueListener}s through
     * {@link java.util.ServiceLoader ServiceLoader} SPI
     */
    public static final String LISTENER_TYPE_AUTO = "auto";

    private AloQueueListenerConfig() {}

    public static <L extends AloQueueListener> Optional<AloQueueListener> load(
            Map<String, ?> properties, Class<L> superType) {
        List<AloQueueListener> listeners = loadExplicit(properties, superType)
                .orElseGet(() -> ConfigLoading.loadListOfConfiguredServices(superType, properties));
        return listeners.isEmpty() ? Optional.empty() : Optional.of(AloQueueListener.combine(listeners));
    }

    private static <L extends AloQueueListener> Optional<List<AloQueueListener>> loadExplicit(
            Map<String, ?> properties, Class<L> superType) {
        return ConfigLoading.loadListOfConfiguredWithPredefinedTypes(
                properties,
                LISTENER_TYPES_CONFIG,
                superType,
                typeName -> instantiatePredefined(properties, superType, typeName));
    }

    private static Optional<List<AloQueueListener>> instantiatePredefined(
            Map<String, ?> properties, Class<? extends AloQueueListener> superType, String typeName) {
        if (typeName.equalsIgnoreCase(LISTENER_TYPE_AUTO)) {
            return Optional.of(ConfigLoading.loadListOfConfiguredServices(superType, properties));
        } else {
            return Optional.empty();
        }
    }
}
