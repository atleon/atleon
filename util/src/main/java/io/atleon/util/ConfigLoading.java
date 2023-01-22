package io.atleon.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Convenience methods for loading configurations from arbitrary String-value mappings
 */
public final class ConfigLoading {

    private ConfigLoading() {

    }

    public static <T extends Configurable> Optional<T> loadConfigured(Map<String, ?> configs, String property) {
        return ConfigLoading.<Class<? extends T>>load(configs, property, TypeResolution::classForQualifiedName)
            .map(clazz -> Instantiation.oneConfigured(clazz, configs));
    }

    public static <T extends Configurable> T loadConfiguredOrThrow(Map<String, ?> configs, String property) {
        return Instantiation.oneConfigured(loadOrThrow(configs, property, TypeResolution::classForQualifiedName), configs);
    }

    public static <T extends Configurable> List<T> loadListOfConfigured(Map<String, ?> configs, String property) {
        List<Class<? extends T>> clazzes = loadListOrEmpty(configs, property, TypeResolution::classForQualifiedName);
        return Instantiation.manyConfigured(clazzes, configs);
    }

    public static <T> T load(Map<String, ?> configs, String property, Function<? super String, T> parser, T defaultValue) {
        return load(configs, property, parser).orElse(defaultValue);
    }

    public static <T> T loadOrThrow(Map<String, ?> configs, String property, Function<? super String, T> parser) {
        return load(configs, property, parser).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static <T> Optional<T> load(Map<String, ?> configs, String property, Function<? super String, T> parser) {
        return Optional.ofNullable(configs.get(property)).map(Object::toString).map(parser);
    }

    public static <T> List<T> loadListOrEmpty(Map<String, ?> configs, String property, Function<? super String, T> parser) {
        return loadCollection(configs, property, parser, Collectors.toList()).orElse(Collections.emptyList());
    }

    public static <T> Set<T> loadSetOrEmpty(Map<String, ?> configs, String property, Function<? super String, T> parser) {
        return loadCollection(configs, property, parser, Collectors.toSet()).orElse(Collections.emptySet());
    }

    public static <T, R> Optional<R> loadCollection(
        Map<String, ?> configs,
        String property,
        Function<? super String, T> parser,
        Collector<? super T, ?, R> collector) {
        return Optional.ofNullable(configs.get(property))
            .map(ConfigLoading::convertToCollection)
            .map(collection -> collection.stream().map(Objects::toString).map(parser).collect(collector));
    }

    public static Map<String, Object> loadPrefixedEnvironmentalProperties(String prefix) {
        return loadPrefixed(Arrays.asList(System.getenv(), System.getProperties()), prefix);
    }

    public static Map<String, Object> loadPrefixed(Map<String, ?> configs, String prefix) {
        return loadPrefixed(Collections.singletonList(configs), prefix);
    }

    private static Map<String, Object> loadPrefixed(List<Map<?, ?>> configsList, String prefix) {
        return configsList.stream()
            .flatMap(configs -> configs.entrySet().stream())
            .filter(entry -> entry.getKey().toString().startsWith(prefix))
            .collect(Collectors.toMap(
                entry -> entry.getKey().toString().substring(prefix.length()),
                Map.Entry::getValue,
                (firstValue, secondValue) -> secondValue
            ));
    }

    private static Collection<?> convertToCollection(Object config) {
        if (config instanceof Collection) {
            return Collection.class.cast(config);
        } else if (config instanceof CharSequence) {
            return Arrays.asList(config.toString().split(","));
        } else {
            return Collections.singletonList(config);
        }
    }

    private static Supplier<RuntimeException> supplyMissingConfigPropertyException(String property) {
        return () -> new IllegalArgumentException("Missing config: " + property);
    }
}
