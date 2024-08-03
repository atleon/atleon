package io.atleon.core;

import io.atleon.util.ConfigLoading;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A reactive Provider that creates configurations for reactive resources. In addition to
 * supporting {@link ConfigInterceptor}s, extensions of this class support {@link ConfigProcessor}s,
 * which allow reactive (non-blocking) enrichment of properties before generating the resulting
 * Config objects.
 *
 * @param <T> The type of Config to reactively produce
 * @param <S> The type of this ConfigFactory
 */
public abstract class ConfigSource<T, S extends ConfigSource<T, S>> extends ConfigProvider<Mono<T>, S> {

    public static final String PROCESSORS_PROPERTY = "atleon.config.processors";

    protected ConfigSource() {

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
        processors.addAll(ConfigLoading.loadListOfInstancesOrEmpty(properties, PROCESSORS_PROPERTY, ConfigProcessor.class));
        return processors;
    }

    protected abstract void validateProperties(Map<String, Object> properties);

    protected abstract T postProcessProperties(Map<String, Object> properties);
}
