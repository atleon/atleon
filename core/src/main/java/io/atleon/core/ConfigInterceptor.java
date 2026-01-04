package io.atleon.core;

import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * Provides or otherwise transforms configurations used to create other resources via Builders or
 * Factories. Implementations should return any provided configurations they don't use, plus any
 * configurations they provide, and omit any configurations they fully consume.
 */
public interface ConfigInterceptor {

    default ConfigProcessor asProcessor() {
        return new ConfigProcessor() {
            @Override
            public Mono<Map<String, Object>> process(String name, Map<String, Object> configs) {
                return Mono.fromCallable(() -> intercept(name, configs));
            }

            @Override
            public Mono<Map<String, Object>> process(Map<String, Object> configs) {
                return Mono.fromCallable(() -> intercept(configs));
            }
        };
    }

    default Map<String, Object> intercept(String name, Map<String, Object> configs) {
        return intercept(configs);
    }

    Map<String, Object> intercept(Map<String, Object> configs);
}
