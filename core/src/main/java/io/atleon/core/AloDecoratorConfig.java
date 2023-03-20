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
     * Optional comma-separated list of {@link AloDecorator} descriptors. Each descriptor is either
     * a predefined type defined in this class or a fully qualified name of a class that implements
     * {@link AloDecorator}. Defaults to {@link #DESCRIPTOR_AUTO}, which results in automatic
     * loading of decorators through the {@link java.util.ServiceLoader} SPI.
     */
    public static final String ALO_DECORATOR_DESCRIPTORS_CONFIG = "alo.decorator.descriptors";

    /**
     * Descriptor used to activate automatic loading of {@link AloDecorator}s through
     * {@link ServiceLoader} SPI
     */
    public static final String DESCRIPTOR_AUTO = "auto";

    private AloDecoratorConfig() {

    }

    public static <T, D extends AloDecorator<T>> Optional<AloDecorator<T>> load(Map<String, ?> properties, Class<D> type) {
        Set<String> descriptors = ConfigLoading.loadSet(properties, ALO_DECORATOR_DESCRIPTORS_CONFIG, Function.identity())
            .orElse(Collections.singleton(DESCRIPTOR_AUTO));
        List<AloDecorator<T>> decorators = descriptors.stream()
            .flatMap(descriptor -> instantiate(type, descriptor))
            .peek(decorator -> decorator.configure(properties))
            .collect(Collectors.toList());
        return decorators.isEmpty() ? Optional.empty() : Optional.of(AloDecorator.combine(decorators));
    }

    private static <T, D extends AloDecorator<T>> Stream<D> instantiate(Class<D> type, String descriptor) {
        if (descriptor.equalsIgnoreCase(DESCRIPTOR_AUTO)) {
            ServiceLoader<D> serviceLoader = ServiceLoader.load(type);
            return StreamSupport.stream(serviceLoader.spliterator(), false);
        } else {
            return Stream.of(descriptor).map(Instantiation::one).map(type::cast);
        }
    }
}
