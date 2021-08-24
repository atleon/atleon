package io.atleon.core;

import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class ConfigFactory<T> {

    public static final String SOURCES_PROPERTY = "sources";

    private final Map<String, Object> properties = new HashMap<>();

    private final Function<Map<String, Object>, Optional<String>> propertiesToName;

    public ConfigFactory() {
        this(properties -> Optional.empty());
    }

    public ConfigFactory(String name) {
        this(properties -> Optional.of(name));
    }

    public ConfigFactory(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }

    public final Mono<T> create() {
        return applySources(new HashMap<>(properties))
            .doOnNext(this::validateProperties)
            .map(this::postProcessProperties);
    }

    public ConfigFactory<T> with(String key, Object value) {
        put(key, value);
        return this;
    }

    public Object put(String key, Object value) {
        return properties.put(key, value);
    }

    protected <R, C extends ConfigFactory<R>> C copyInto(Supplier<C> copySupplier) {
        C copy = copySupplier.get();
        properties.forEach(copy::put);
        return copy;
    }

    protected Mono<Map<String, Object>> applySources(Map<String, Object> properties) {
        Optional<String> nameFromProperties = propertiesToName.apply(properties);
        Mono<Map<String, Object>> result = Mono.just(properties);
        for (ConfigSource source : loadSources(properties)) {
            result = result.flatMap(configs ->
                nameFromProperties
                    .map(name -> source.obtain(name, configs))
                    .orElseGet(() -> source.obtain(configs)));
        }
        return result;
    }

    protected List<ConfigSource> loadSources(Map<String, Object> properties) {
        List<ConfigSource> sources = new ArrayList<>();
        sources.add(new EnvironmentalConfigs().asSource());
        sources.add(new ConditionallyRandomizedConfigs().asSource());
        ConfigLoading.loadCollection(properties, SOURCES_PROPERTY, Instantiation::<ConfigSource>one, Collectors.toList())
            .ifPresent(sources::addAll);
        return sources;
    }

    protected abstract void validateProperties(Map<String, Object> properties);

    protected abstract T postProcessProperties(Map<String, Object> properties);

    protected static void validateNonNullProperty(Map<String, Object> properties, String key) {
        Objects.requireNonNull(properties.get(key), key + " is a required Configuration");
    }

    protected static <T extends Enum<T>> void validateEnumProperty(Map<String, Object> properties, String key, Class<T> enumClass) {
        try {
            Enum.valueOf(enumClass, Objects.toString(properties.get(key)));
        } catch (Exception e) {
            throw new IllegalArgumentException(key + " must be configured as an Enum value from " + enumClass, e);
        }
    }
}
