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
import java.util.stream.Stream;

/**
 * Convenience methods for loading configurations from arbitrary String-value mappings
 */
public final class ConfigLoading {

    private ConfigLoading() {

    }

    public static <T extends Configurable> Optional<T> loadConfigured(Map<String, ?> configs, String property) {
        return ConfigLoading.<Class<? extends T>>load(configs, property, TypeResolution::classForQualifiedName)
            .map(clazz -> createConfigured(configs, clazz));
    }

    public static <T extends Configurable> T loadConfiguredOrThrow(Map<String, ?> configs, String property) {
        return createConfigured(configs, loadOrThrow(configs, property, TypeResolution::classForQualifiedName));
    }

    public static <T extends Configurable> List<T> loadListOfConfigured(Map<String, ?> configs, String property) {
        List<T> configurables = loadListOrEmpty(configs, property, Instantiation::one);
        configurables.forEach(interceptor -> interceptor.configure(configs));
        return configurables;
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

    public static <T, R> R loadCollectionOrThrow(
        Map<String, ?> configs,
        String property,
        Function<? super String, T> parser,
        Collector<? super T, ?, R> collector) {
        return loadCollection(configs, property, parser, collector).orElseThrow(supplyMissingConfigPropertyException(property));
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
        return Stream.of(System.getenv().entrySet(), System.getProperties().entrySet())
            .flatMap(Collection::stream)
            .filter(entry -> Objects.toString(entry.getKey()).startsWith(prefix))
            .collect(Collectors.toMap(
                entry -> entry.getKey().toString().substring(prefix.length()),
                Map.Entry::getValue,
                (firstValue, secondValue) -> secondValue));
    }

    public static <T> Map<String, T> loadPrefixed(Map<String, ?> configs, String prefix, Function<? super String, T> parser) {
        return configs.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .collect(Collectors.toMap(
                entry -> entry.getKey().substring(prefix.length()),
                entry -> parser.apply(entry.getValue().toString())));
    }

    private static <T extends Configurable> T createConfigured(Map<String, ?> configs, Class<? extends T> clazz) {
        T configurable = Instantiation.one(clazz);
        configurable.configure(configs);
        return configurable;
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
