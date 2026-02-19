package io.atleon.core;

import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestConfigProcessor implements ConfigProcessor {

    public static final String TEST_KEY = "testKey";

    public static final String TEST_VALUE = "testValue";

    private static final AtomicInteger COUNT = new AtomicInteger(0);

    @Override
    public Mono<Map<String, Object>> process(Map<String, Object> configs) {
        return Mono.fromCallable(COUNT::incrementAndGet).map(count -> {
            Map<String, Object> result = new HashMap<>(configs);
            result.put(TEST_KEY + count, TEST_VALUE + count);
            return result;
        });
    }

    public static void resetCount() {
        COUNT.set(0);
    }
}
