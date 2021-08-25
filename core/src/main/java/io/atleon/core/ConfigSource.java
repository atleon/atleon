package io.atleon.core;

import io.atelon.util.ConfigLoading;
import io.atelon.util.Instantiation;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A Config Producer that reactively provides configurations for reactive resources
 *
 * @param <T> The type of Config to reactively produce
 * @param <S> The type of this ConfigFactory
 */
public abstract class ConfigSource<T, S extends ConfigSource<T, S>> extends ConfigProducer<S> {

    public static final String PROCESSORS_PROPERTY = "sources";

    private final Function<Map<String, Object>, Optional<String>> propertiesToName;

    public ConfigSource() {
        this(properties -> Optional.empty());
    }

    public ConfigSource(String name) {
        this(properties -> Optional.of(name));
    }

    public ConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }

    public final Mono<T> create() {
        return applySources(new HashMap<>(properties))
            .doOnNext(this::validateProperties)
            .map(this::postProcessProperties);
    }

    protected abstract void validateProperties(Map<String, Object> properties);

    protected abstract T postProcessProperties(Map<String, Object> properties);

    protected Mono<Map<String, Object>> applySources(Map<String, Object> properties) {
        Optional<String> nameFromProperties = propertiesToName.apply(properties);
        Mono<Map<String, Object>> result = Mono.just(properties);
        for (ConfigProcessor source : loadSources(properties)) {
            result = result.flatMap(configs ->
                nameFromProperties
                    .map(name -> source.process(name, configs))
                    .orElseGet(() -> source.process(configs)));
        }
        return result;
    }

    protected List<ConfigProcessor> loadSources(Map<String, Object> properties) {
        List<ConfigProcessor> sources = new ArrayList<>();
        sources.add(new EnvironmentalConfigs().asProcessor());
        sources.add(new ConditionallyRandomizedConfigs().asProcessor());
        ConfigLoading.loadCollection(properties, PROCESSORS_PROPERTY, Instantiation::<ConfigProcessor>one, Collectors.toList())
            .ifPresent(sources::addAll);
        return sources;
    }
}
