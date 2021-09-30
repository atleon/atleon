package io.atleon.core;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Instantiation;
import reactor.core.publisher.Mono;

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

    public static final String PROCESSORS_PROPERTY = "source.processors";

    protected ConfigSource() {
        super();
    }

    protected ConfigSource(String name) {
        super(name);
    }

    protected ConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        super(propertiesToName);
    }

    @Override
    protected final Mono<T> create(String name, Map<String, Object> properties) {
        return applyProcessors(name, properties)
            .doOnNext(this::validateProperties)
            .map(this::postProcessProperties);
    }

    @Override
    protected final Mono<T> create(Map<String, Object> properties) {
        return applyProcessors(properties)
            .doOnNext(this::validateProperties)
            .map(this::postProcessProperties);
    }

    protected final Mono<Map<String, Object>> applyProcessors(String name, Map<String, Object> properties) {
        Mono<Map<String, Object>> result = Mono.just(properties);
        for (ConfigProcessor processor : loadProcessors(properties)) {
            result = result.flatMap(configs -> processor.process(name, configs));
        }
        return result;
    }

    protected final Mono<Map<String, Object>> applyProcessors(Map<String, Object> properties) {
        Mono<Map<String, Object>> result = Mono.just(properties);
        for (ConfigProcessor processor : loadProcessors(properties)) {
            result = result.flatMap(processor::process);
        }
        return result;
    }

    protected List<ConfigProcessor> loadProcessors(Map<String, Object> properties) {
        List<ConfigProcessor> processors = defaultInterceptors().stream()
            .map(ConfigInterceptor::asProcessor)
            .collect(Collectors.toList());
        processors.addAll(ConfigLoading.loadListOrEmpty(properties, PROCESSORS_PROPERTY, Instantiation::one));
        return processors;
    }

    protected abstract void validateProperties(Map<String, Object> properties);

    protected abstract T postProcessProperties(Map<String, Object> properties);
}
