package io.atleon.core;

import io.atleon.util.ConfigLoading;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for loading {@link AloDecorator}s
 */
public final class AloDecoratorConfig {

    /**
     * Optional comma-separated list of {@link AloDecorator} types. Each type is either a
     * predefined type defined in this class or a fully qualified name of a class that implements
     * {@link AloDecorator}. Defaults to {@link #DECORATOR_TYPE_AUTO}, which results in automatic
     * loading of decorators through the {@link java.util.ServiceLoader} SPI.
     */
    public static final String ALO_DECORATOR_TYPES_CONFIG = "alo.decorator.types";

    /**
     * Type name used to activate automatic loading of {@link AloDecorator}s through
     * {@link java.util.ServiceLoader ServiceLoader} SPI
     */
    public static final String DECORATOR_TYPE_AUTO = "auto";

    private AloDecoratorConfig() {

    }

    public static <T, D extends AloDecorator<T>> Optional<AloDecorator<T>>
    load(Map<String, ?> properties, Class<D> superType) {
        List<AloDecorator<T>> decorators = loadExplicit(properties, superType)
            .orElseGet(() -> ConfigLoading.loadListOfConfiguredServices(superType, properties));
        return decorators.isEmpty() ? Optional.empty() : Optional.of(AloDecorator.combine(decorators));
    }

    private static <T, D extends AloDecorator<T>> Optional<List<AloDecorator<T>>>
    loadExplicit(Map<String, ?> properties, Class<D> superType) {
        return ConfigLoading.loadListOfConfiguredWithPredefinedTypes(
            properties,
            ALO_DECORATOR_TYPES_CONFIG,
            superType,
            typeName -> instantiatePredefined(properties, superType, typeName)
        );
    }

    private static <T> Optional<List<AloDecorator<T>>>
    instantiatePredefined(Map<String, ?> properties, Class<? extends AloDecorator<T>> superType, String typeName) {
        if (typeName.equalsIgnoreCase(DECORATOR_TYPE_AUTO)) {
            return Optional.of(ConfigLoading.loadListOfConfiguredServices(superType, properties));
        } else {
            return Optional.empty();
        }
    }
}
