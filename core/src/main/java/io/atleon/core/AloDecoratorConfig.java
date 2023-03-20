package io.atleon.core;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Instantiation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
     * {@link ServiceLoader} SPI
     */
    public static final String DECORATOR_TYPE_AUTO = "auto";

    private AloDecoratorConfig() {

    }

    public static <T, D extends AloDecorator<T>> Optional<AloDecorator<T>> load(
        Map<String, ?> properties,
        Class<D> superType
    ) {
        Set<String> types = ConfigLoading.loadSet(properties, ALO_DECORATOR_TYPES_CONFIG, Function.identity())
            .orElse(Collections.singleton(DECORATOR_TYPE_AUTO));
        List<AloDecorator<T>> decorators = types.stream()
            .flatMap(descriptor -> instantiate(superType, descriptor))
            .peek(decorator -> decorator.configure(properties))
            .collect(Collectors.toList());
        return decorators.isEmpty() ? Optional.empty() : Optional.of(AloDecorator.combine(decorators));
    }

    private static <T, D extends AloDecorator<T>> Stream<D> instantiate(Class<D> superType, String type) {
        if (type.equalsIgnoreCase(DECORATOR_TYPE_AUTO)) {
            ServiceLoader<D> serviceLoader = ServiceLoader.load(superType);
            return StreamSupport.stream(serviceLoader.spliterator(), false);
        } else {
            return Stream.of(type).map(Instantiation::one).map(superType::cast);
        }
    }
}
