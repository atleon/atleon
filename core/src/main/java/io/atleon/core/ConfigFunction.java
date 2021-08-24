package io.atleon.core;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Provides or otherwise transforms configurations used to create other resources via Builders or
 * Factories. Implementations should return any provided configurations they don't use, plus any
 * configurations they provide, and omit any configurations they fully consume.
 */
public interface ConfigFunction {

    default ConfigSource asSource() {
        return new ConfigSource() {
            @Override
            public Mono<Map<String, Object>> obtain(String name, Map<String, Object> configs) {
                return Mono.fromCallable(() -> apply(name, configs));
            }

            @Override
            public Mono<Map<String, Object>> obtain(Map<String, Object> configs) {
                return Mono.fromCallable(() -> apply(configs));
            }
        };
    }

    default Map<String, Object> apply(String name, Map<String, Object> configs) {
        return apply(configs);
    }

    Map<String, Object> apply(Map<String, Object> configs);
}
