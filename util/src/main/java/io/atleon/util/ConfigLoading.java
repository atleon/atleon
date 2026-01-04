package io.atleon.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

    private ConfigLoading() {}

    /**
     * Strips out conventional Atleon-specific properties to distill "native" config
     */
    public static Map<String, Object> loadNative(Map<String, ?> configs) {
        return configs.entrySet().stream()
                .filter(entry -> !entry.getKey().matches("^((atleon\\.)|(\\w+\\.(bounded\\.)?(receiver|sender)\\.)).+"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <T extends Configurable> T loadConfiguredOrThrow(
            Map<String, ?> configs, String property, Class<? extends T> type) {
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

    public static <T extends Enum<T>> T loadEnumOrThrow(Map<String, ?> configs, String property, Class<T> enumType) {
        return loadEnum(configs, property, enumType).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static Class<?> loadClassOrThrow(Map<String, ?> configs, String property) {
        return ConfigLoading.loadClass(configs, property).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static <T extends Configurable> Optional<T> loadConfiguredWithPredefinedTypes(
            Map<String, ?> configs,
            String property,
            Class<? extends T> type,
            Function<String, Optional<T>> predefinedTypeInstantiator) {
        Optional<T> result = loadInstanceWithPredefinedTypes(configs, property, type, predefinedTypeInstantiator);
        result.ifPresent(configurable -> configurable.configure(configs));
        return result;
    }

    public static <T> Optional<T> loadInstanceWithPredefinedTypes(
            Map<String, ?> configs,
            String property,
            Class<? extends T> type,
            Function<String, Optional<T>> predefinedTypeInstantiator) {
        Function<String, T> instantiator = typeName ->
                predefinedTypeInstantiator.apply(typeName).orElseGet(() -> Instantiation.oneTyped(type, typeName));
        return load(configs, property, value -> coerce(value, type, instantiator));
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

    public static <T extends Enum<T>> Optional<T> loadEnum(Map<String, ?> configs, String property, Class<T> enumType) {
        return loadParseable(configs, property, enumType, value -> parseEnum(enumType, value));
    }

    public static Optional<Class<?>> loadClass(Map<String, ?> configs, String property) {
        return loadParseable(configs, property, Class.class, TypeResolution::classForQualifiedName)
                .map(type -> (Class<?>) type);
    }

    public static <T> Optional<T> loadParseable(
            Map<String, ?> configs, String property, Class<T> type, Function<? super String, T> parser) {
        return load(configs, property, value -> coerce(value, type, parser));
    }

    public static <T extends Configurable> List<T> loadListOfConfiguredServices(
            Class<? extends T> type, Map<String, ?> configs) {
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
            Function<String, Optional<List<T>>> predefinedTypeInstantiator) {
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
            Function<String, Optional<List<T>>> predefinedTypeInstantiator) {
        return loadStream(configs, property, Function.identity())
                .map(stream -> coerceToList(stream, type, predefinedTypeInstantiator));
    }

    public static Optional<Set<String>> loadSetOfString(Map<String, ?> configs, String property) {
        return loadStream(configs, property, Objects::toString)
                .map(stream -> stream.collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    private static <T extends Enum<T>> T parseEnum(Class<T> enumType, String value) {
        try {
            Method method = enumType.getDeclaredMethod("valueOf", String.class);
            return enumType.cast(method.invoke(null, value));
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Could not get parsing method from enumType=" + enumType, e);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke parsing method from enumType=" + enumType, e);
        }
    }

    private static <T> Optional<T> load(Map<String, ?> configs, String property, Function<Object, T> coercer) {
        return Optional.ofNullable(configs.get(property)).map(coercer);
    }

    private static <T> Optional<Stream<T>> loadStream(
            Map<String, ?> configs, String property, Function<Object, T> coercer) {
        if (configs.containsKey(property)) {
            Object value = configs.get(property);
            return Optional.of(
                    value == null ? Stream.empty() : convertToStream(value).map(coercer));
        } else {
            return Optional.empty();
        }
    }

    private static Stream<?> convertToStream(Object value) {
        if (value instanceof Collection) {
            return Collection.class.cast(value).stream();
        } else if (value instanceof CharSequence) {
            return Stream.of(value.toString().split(",")).map(String::trim).filter(string -> !string.isEmpty());
        } else {
            return Stream.of(value);
        }
    }

    private static <T> List<T> coerceToList(
            Stream<Object> stream,
            Class<? extends T> type,
            Function<String, Optional<List<T>>> predefinedTypeInstantiator) {
        Function<String, T> instantiator = typeName -> Instantiation.oneTyped(type, typeName);
        return stream.flatMap(object -> coerceToList(object, type, instantiator, predefinedTypeInstantiator).stream())
                .collect(Collectors.toList());
    }

    private static <T> List<T> coerceToList(
            Object value,
            Class<? extends T> toType,
            Function<? super String, T> parser,
            Function<String, Optional<List<T>>> predefinedTypeInstantiator) {
        Optional<List<T>> instantiated =
                value instanceof CharSequence ? predefinedTypeInstantiator.apply(value.toString()) : Optional.empty();
        return instantiated.orElseGet(() -> Collections.singletonList(coerce(value, toType, parser)));
    }

    private static <T> T coerce(Object value, Class<? extends T> toType, Function<? super String, T> parser) {
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
