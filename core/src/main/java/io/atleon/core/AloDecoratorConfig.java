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
     * Descriptor used to activate automatic loading of {@link AloDecorator}s through
     * {@link ServiceLoader} SPI
     */
    public static final String DESCRIPTOR_AUTO = "auto";

    private AloDecoratorConfig() {

    }

    public static <T, C extends AloDecorator<T>> Optional<AloDecorator<T>> load(
        Class<C> type,
        Map<String, ?> properties,
        String descriptorProperty
    ) {
        Set<String> descriptors = ConfigLoading.loadSet(properties, descriptorProperty, Function.identity())
            .orElse(Collections.singleton(DESCRIPTOR_AUTO));
        List<AloDecorator<T>> decorators = descriptors.stream()
            .flatMap(descriptor -> instantiate(type, descriptor))
            .peek(decorator -> decorator.configure(properties))
            .collect(Collectors.toList());
        return decorators.isEmpty() ? Optional.empty() : Optional.of(AloDecorator.combine(decorators));
    }

    private static <T, C extends AloDecorator<T>> Stream<C> instantiate(Class<C> type, String descriptor) {
        if (descriptor.equalsIgnoreCase(DESCRIPTOR_AUTO)) {
            ServiceLoader<C> serviceLoader = ServiceLoader.load(type);
            return StreamSupport.stream(serviceLoader.spliterator(), false);
        } else {
            return Stream.of(descriptor).map(Instantiation::one).map(type::cast);
        }
    }
}
