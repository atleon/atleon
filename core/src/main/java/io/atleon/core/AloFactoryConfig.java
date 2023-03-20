package io.atleon.core;

import java.util.Map;

/**
 * Utility for loading {@link AloFactory} implementations
 */
public final class AloFactoryConfig {

    private AloFactoryConfig() {

    }

    public static <T, D extends AloDecorator<T>> AloFactory<T> loadDecorated(
        Map<String, ?> properties,
        Class<D> decoratorType
    ) {
        AloFactory<T> aloFactory = loadDefault();
        return AloDecoratorConfig.load(properties, decoratorType)
            .map(aloFactory::withDecorator)
            .orElse(aloFactory);
    }

    public static <T> AloFactory<T> loadDefault() {
        return ComposedAlo.factory();
    }
}
