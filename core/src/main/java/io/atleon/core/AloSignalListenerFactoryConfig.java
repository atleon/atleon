package io.atleon.core;

import io.atleon.util.ConfigLoading;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for loading {@link AloSignalListenerFactory} instances.
 */
public final class AloSignalListenerFactoryConfig {

    /**
     * Optional comma-separated list of {@link AloSignalListenerFactory} types. Each type is either
     * a predefined type defined in this class or a fully qualified name of a class that implements
     * {@link AloSignalListenerFactory}. Defaults to {@link #SIGNAL_LISTENER_FACTORY_TYPE_AUTO},
     * which results in automatic loading of decorators through the {@link java.util.ServiceLoader}
     * SPI.
     */
    public static final String SIGNAL_LISTENER_FACTORY_TYPES_CONFIG = "alo.signal.listener.factory.types";

    /**
     * Type name used to activate automatic loading of {@link AloSignalListenerFactory}s through
     * {@link java.util.ServiceLoader ServiceLoader} SPI
     */
    public static final String SIGNAL_LISTENER_FACTORY_TYPE_AUTO = "auto";

    private AloSignalListenerFactoryConfig() {

    }

    public static <T, D extends AloSignalListenerFactory<T, ?>> List<AloSignalListenerFactory<T, ?>>
    loadList(Map<String, ?> properties, Class<D> superType) {
        return loadExplicitList(properties, superType)
            .orElseGet(() -> ConfigLoading.loadListOfConfiguredServices(superType, properties));
    }

    private static <T, D extends AloSignalListenerFactory<T, ?>> Optional<List<AloSignalListenerFactory<T, ?>>>
    loadExplicitList(Map<String, ?> properties, Class<D> superType) {
        return ConfigLoading.loadListOfConfiguredWithPredefinedTypes(
            properties,
            SIGNAL_LISTENER_FACTORY_TYPES_CONFIG,
            superType,
            typeName -> instantiatePredefined(properties, superType, typeName)
        );
    }

    private static <T> Optional<List<AloSignalListenerFactory<T, ?>>> instantiatePredefined(
        Map<String, ?> properties,
        Class<? extends AloSignalListenerFactory<T, ?>> superType,
        String typeName
    ) {
        if (typeName.equalsIgnoreCase(SIGNAL_LISTENER_FACTORY_TYPE_AUTO)) {
            return Optional.of(ConfigLoading.loadListOfConfiguredServices(superType, properties));
        } else {
            return Optional.empty();
        }
    }
}
