package io.atleon.core;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Instantiation;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A reactive Provider that creates configurations for reactive resources
 *
 * @param <T> The type of Config to reactively produce
 * @param <S> The type of this ConfigFactory
 */
public abstract class ConfigSource<T, S extends ConfigSource<T, S>> extends ConfigProvider<Mono<T>, S> {

    public static final String PROCESSORS_PROPERTY = "config.processors";

    private Function<Map<String, Object>, Optional<String>> propertiesToName;

    protected ConfigSource() {
        this(properties -> Optional.empty());
    }

    protected ConfigSource(String name) {
        this(properties -> Optional.of(name));
    }

    protected ConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }

    @Override
    public final Mono<T> create(Map<String, Object> properties) {
        return applySources(properties)
            .doOnNext(this::validateProperties)
            .map(this::postProcessProperties);
    }

    @Override
    protected final S initializeProviderCopy() {
        S copy = initializeSourceCopy();
        copy.setPropertiesToName(propertiesToName);
        return copy;
    }

    protected abstract S initializeSourceCopy();

    protected abstract void validateProperties(Map<String, Object> properties);

    protected abstract T postProcessProperties(Map<String, Object> properties);

    protected Mono<Map<String, Object>> applySources(Map<String, Object> properties) {
        Optional<String> nameFromProperties = propertiesToName.apply(properties);
        Mono<Map<String, Object>> result = Mono.just(properties);
        for (ConfigProcessor processor : loadProcessors(properties)) {
            result = result.flatMap(configs ->
                nameFromProperties
                    .map(name -> processor.process(name, configs))
                    .orElseGet(() -> processor.process(configs)));
        }
        return result;
    }

    protected List<ConfigProcessor> loadProcessors(Map<String, Object> properties) {
        List<ConfigProcessor> processors = new ArrayList<>(defaultProcessors());
        ConfigLoading.loadCollection(properties, PROCESSORS_PROPERTY, Instantiation::<ConfigProcessor>one, Collectors.toList())
            .ifPresent(processors::addAll);
        return processors;
    }

    protected Collection<ConfigProcessor> defaultProcessors() {
        return Arrays.asList(
            new EnvironmentalConfigs().asProcessor(),
            new ConditionallyRandomizedConfigs().asProcessor());
    }

    protected void setPropertiesToName(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        this.propertiesToName = propertiesToName;
    }
}
