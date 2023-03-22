package io.atleon.util;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Convenience methods for loading configurations from arbitrary String-value mappings
 */
public final class ConfigLoading {

    private ConfigLoading() {

    }

    public static <T extends Configurable> T
    loadConfiguredOrThrow(Map<String, ?> configs, String property, Class<? extends T> type) {
        return loadConfiguredWithPredefinedTypes(configs, property, type, typeName -> Optional.empty())
            .orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static Duration loadDurationOrThrow(Map<String, ?> configs, String property) {
        return loadDuration(configs, property).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static boolean loadBooleanOrThrow(Map<String, ?> configs, String property) {
        return loadBoolean(configs, property).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static int loadIntOrThrow(Map<String, ?> configs, String property) {
        return loadInt(configs, property).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static long loadLongOrThrow(Map<String, ?> configs, String property) {
        return loadLong(configs, property).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static String loadStringOrThrow(Map<String, ?> configs, String property) {
        return loadString(configs, property).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static <T extends Configurable> Optional<T> loadConfiguredWithPredefinedTypes(
        Map<String, ?> configs,
        String property,
        Class<? extends T> type,
        Function<String, Optional<T>> predefinedTypeInstantiator
    ) {
        Function<String, T> instantiator = newInstantiatorWithPredefinedTypes(type, predefinedTypeInstantiator);
        Optional<T> result = load(configs, property, value -> coerce(type, value, instantiator));
        result.ifPresent(configurable -> configurable.configure(configs));
        return result;
    }

    public static Optional<URI> loadUri(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, URI.class, URI::create);
    }

    public static Optional<Duration> loadDuration(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, Duration.class, Duration::parse);
    }

    public static Optional<Boolean> loadBoolean(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, Boolean.class, Boolean::parseBoolean);
    }

    public static Optional<Integer> loadInt(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, Number.class, Integer::parseInt).map(Number::intValue);
    }

    public static Optional<Long> loadLong(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, Number.class, Long::parseLong).map(Number::longValue);
    }

    public static Optional<String> loadString(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, String.class, Function.identity());
    }

    public static <T> Optional<T>
    loadParseable(Map<String, ?> configs, String property, Class<T> type, Function<? super String, T> parser) {
        return load(configs, property, value -> coerce(type, value, parser));
    }

    public static <T extends Configurable> List<T>
    loadListOfConfiguredServices(Class<? extends T> type, Map<String, ?> configs) {
        ServiceLoader<? extends T> serviceLoader = ServiceLoader.load(type);
        return StreamSupport.stream(serviceLoader.spliterator(), false)
            .peek(configurable -> configurable.configure(configs))
            .collect(Collectors.toList());
    }

    public static <T> List<T> loadListOfInstancesOrEmpty(Map<String, ?> configs, String property, Class<T> type) {
        return loadListOfInstances(configs, property, type).orElse(Collections.emptyList());
    }

    public static Set<String> loadSetOfStringOrEmpty(Map<String, ?> configs, String property) {
        return loadSetOfString(configs, property).orElse(Collections.emptySet());
    }

    public static <T extends Configurable> Optional<List<T>> loadListOfConfiguredWithPredefinedTypes(
        Map<String, ?> configs,
        String property,
        Class<? extends T> type,
        Function<String, Optional<T>> predefinedTypeInstantiator
    ) {
        Optional<List<T>> result =
            loadListOfInstancesWithPredefinedTypes(configs, property, type, predefinedTypeInstantiator);
        result.ifPresent(configurables -> configurables.forEach(configurable -> configurable.configure(configs)));
        return result;
    }

    public static <T> Optional<List<T>> loadListOfInstances(Map<String, ?> configs, String property, Class<T> type) {
        return loadListOfInstancesWithPredefinedTypes(configs, property, type, typeName -> Optional.empty());
    }

    public static <T> Optional<List<T>> loadListOfInstancesWithPredefinedTypes(
        Map<String, ?> configs,
        String property,
        Class<? extends T> type,
        Function<String, Optional<T>> predefinedTypeInstantiator
    ) {
        Function<String, T> instantiator = newInstantiatorWithPredefinedTypes(type, predefinedTypeInstantiator);
        return ConfigLoading.loadStream(configs, property, value -> coerce(type, value, instantiator))
            .map(stream -> stream.collect(Collectors.toList()));
    }

    public static Optional<Set<String>> loadSetOfString(Map<String, ?> configs, String property) {
        return loadStream(configs, property, Objects::toString)
            .map(stream -> stream.collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    private static <T> Optional<T> load(Map<String, ?> configs, String property, Function<Object, T> coercer) {
        return Optional.ofNullable(configs.get(property)).map(coercer);
    }

    private static <T> Optional<Stream<T>> loadStream(Map<String, ?> configs, String property, Function<Object, T> coercer) {
        return Optional.ofNullable(configs.get(property))
            .map(value -> convertToStream(value).map(coercer));
    }

    private static Stream<?> convertToStream(Object value) {
        if (value instanceof Collection) {
            return Collection.class.cast(value).stream();
        } else if (value instanceof CharSequence) {
            return Stream.of(value.toString().split(",")).map(String::trim);
        } else {
            return Stream.of(value);
        }
    }

    private static <T> Function<String, T>
    newInstantiatorWithPredefinedTypes(Class<? extends T> type, Function<String, Optional<T>> predefinedTypeInstantiator) {
        return typeName -> predefinedTypeInstantiator.apply(typeName).orElseGet(() -> Instantiation.oneTyped(type, typeName));
    }

    private static <T> T coerce(Class<? extends T> toType, Object value, Function<? super String, T> parser) {
        if (toType.isInstance(value)) {
            return toType.cast(value);
        } else if (value instanceof Class) {
            return Instantiation.oneTyped(toType, Class.class.cast(value));
        } else if (value instanceof CharSequence) {
            return parser.apply(value.toString());
        } else {
            throw new UnsupportedOperationException("Cannot coerce value=" + value + " to object of type=" + toType);
        }
    }

    private static Supplier<RuntimeException> supplyMissingConfigPropertyException(String property) {
        return () -> new IllegalArgumentException("Missing config: " + property);
    }
}
