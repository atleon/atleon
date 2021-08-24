package io.atleon.core;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Reactively provides or otherwise transforms configurations used to create other resources via
 * Factories. Implementations should return any provided configurations they don't use, plus any
 * configurations they provide, and omit any configurations they fully consume.
 */
public interface ConfigSource {

    default Mono<Map<String, Object>> obtain(String name, Map<String, Object> configs) {
        return obtain(configs);
    }

    Mono<Map<String, Object>> obtain(Map<String, Object> configs);
}
